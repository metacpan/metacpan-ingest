use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS qw< decode_json >;
use DateTime         ();
use Email::Valid     ();
use Encode           ();
use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;
use URI              ();

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    author_dir
    cpan_dir
    cpan_file_map
    diff_struct
    read_00whois
>;

# config
my @author_config_fields = qw<
    name
    asciiname
    profile
    blog
    perlmongers
    donation
    email
    website
    city
    region
    country
    location
    extra
>;

my @cpan_fields = qw<
    pauseid
    name
    email
    website
    asciiname
    is_pause_custodial_account
>;

my @compare_fields = do {
    my %seen;
    sort grep !$seen{$_}++, @cpan_fields, @author_config_fields;
};

# args
my ( $findls_file, $pauseid, $whois_file );
GetOptions(
    "findls_file=s" => \$findls_file,
    "pauseid=s"     => \$pauseid,
    "whois_file=s"  => \$whois_file,
);

# setup
my $es = MetaCPAN::ES->new( index => "author" );

log_info {'Reading 00whois'};
my $authors_data = read_00whois($whois_file);

if ($pauseid) {
    log_info {"Indexing 1 author"};
    $authors_data = { $pauseid => $authors_data->{$pauseid} };
}
else {
    my $count = keys %$authors_data;
    log_debug {"Counting author"};
    log_info {"Indexing $count authors"};
}

my @author_ids_to_purge;

my $bulk   = $es->bulk( max_count => 250, timeout => '25m' );
my $scroll = $es->scroll(
    size => 500,
    body => {
        query => {
            $pauseid
            ? ( term => { pauseid => $pauseid } )
            : ( match_all => {} ),
        },
        _source => [@compare_fields],
    }
);

update_authors();
new_authors();

$bulk->flush;
$es->index_refresh;

#$self->perform_purges;

log_info {"done"};

sub _update_author ( $id, $whois_data, $current_data ) {
    my $data = _author_data_from_cpan( $id, $whois_data );

    log_debug {
        Encode::encode_utf8( sprintf(
            "Indexing %s: %s <%s>",
            $id, $data->{name}, $data->{email}
        ) )
    };

    ### validate data (previously ESX::Model)

    if ( my $diff = diff_struct( $current_data, $data, 1 ) ) {
        Dlog_debug {"Found difference in $id: $_"} $diff
            if $current_data;
    }
    else {
        return;
    }

    $data->{updated} = DateTime->now( time_zone => 'UTC' )->iso8601;

    $bulk->update( {
        id            => $id,
        doc           => $data,
        doc_as_upsert => 1,
    } );

    push @author_ids_to_purge, $id;
}

sub _author_data_from_cpan ( $id, $whois_data ) {
    my $author_config = _author_config($id) || {};

    my $data = {
        pauseid   => $id,
        name      => $whois_data->{fullname},
        email     => $whois_data->{email},
        website   => $whois_data->{homepage},
        asciiname => $whois_data->{asciiname},
        %$author_config,
        is_pause_custodial_account => (
            ( $whois_data->{fullname} // '' )
            =~ /\(PAUSE Custodial Account\)/ ? 1 : 0
        ),
    };

    undef $data->{name}
        if ref $data->{name};

    $data->{name} = $id
        unless length $data->{name};

    $data->{asciiname} = q{}
        unless defined $data->{asciiname};

    $data->{email} = lc($id) . '@cpan.org'
        unless $data->{email} && Email::Valid->address( $data->{email} );

    $data->{website} = [

        # normalize www.homepage.com to http://www.homepage.com
        map +( $_->scheme ? '' : 'http://' ) . $_->as_string,
        map URI->new($_)->canonical,
        grep $_,
        map +( ref eq 'ARRAY' ? @$_ : $_ ),
        $data->{website}
    ];

    # Do not import lat / lon's in the wrong order, or just invalid
    if ( my $loc = $data->{location} ) {
        if ( ref $loc ne 'ARRAY' || @$loc != 2 ) {
            delete $data->{location};
        }
        else {
            my $lat = $loc->[1];
            my $lon = $loc->[0];

            if ( !defined $lat or $lat > 90 or $lat < -90 ) {

                # Invalid latitude
                delete $data->{location};
            }
            elsif ( !defined $lon or $lon > 180 or $lon < -180 ) {

                # Invalid longitude
                delete $data->{location};
            }
        }
    }

    return $data;
}

sub _author_config ($id) {
    my $cpan = cpan_dir();
    my $dir  = $cpan->child( 'authors', author_dir($id) );
    return undef
        unless $dir->is_dir;

    my $cpan_file_map     = cpan_file_map($findls_file);
    my $author_cpan_files = $cpan_file_map->{$id}
        or return undef;

    # Get the most recent version
    my ($file) = map $_->[0], sort { $b->[1] <=> $a->[1] }
        map [ $_ => $_->stat->mtime ],
        grep $author_cpan_files->{ $_->basename },
        $dir->children(qr/\Aauthor-.*\.json\z/);

    return undef
        unless $file;

    my $author;
    eval {
        $author = decode_json( $file->slurp_raw );
        1;
    } or do {
        log_warn {"$file is broken: $@"};
        return undef;
    };

    return {
        map {
            my $value = $author->{$_};
            defined $value ? ( $_ => $value ) : ()
        } @author_config_fields
    };
}

sub update_authors () {
    while ( my $doc = $scroll->next ) {
        my $id         = $doc->{_id};
        my $whois_data = delete $authors_data->{$id} || next;
        _update_author( $id, $whois_data, $doc->{_source} );
    }
}

sub new_authors () {
    for my $id ( keys %$authors_data ) {
        my $whois_data = delete $authors_data->{$id} || next;
        _update_author( $id, $whois_data, {} );
    }
}

1;

__END__
