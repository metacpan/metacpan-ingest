package MetaCPAN::Ingest;

use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS   ();
use Digest::SHA        ();
use File::Basename     ();
use File::Spec         ();
use LWP::UserAgent     ();
use PAUSE::Permissions ();
use Encode             qw( decode_utf8 );
use IO::Prompt::Tiny   qw( prompt );
use Path::Tiny         qw( path );
use Ref::Util          qw( is_plain_arrayref is_plain_hashref is_ref );
use Scalar::Util       qw( blessed );
use Term::ANSIColor    qw( colored );
use XML::Simple        qw( XMLin );

use MetaCPAN::Config ();
use MetaCPAN::Logger qw( log_debug log_error log_fatal log_info );

use Sub::Exporter -setup => {
    exports => [ qw<
        are_you_sure
        config
        author_dir
        cpan_dir
        cpan_file_map
        diff_struct
        digest
        download_url
        es_config
        extract_section
        false
        fix_version
        handle_error
        home
        is_bool
        is_dev
        minion
        numify_version
        read_00whois
        read_02packages
        read_02packages_fh
        read_06perms_fh
        read_06perms_iter
        read_recent_segment
        read_url
        strip_pod
        tmp_dir
        true
        ua
    > ]
};

*true    = \&Cpanel::JSON::XS::true;
*false   = \&Cpanel::JSON::XS::false;
*is_bool = \&Cpanel::JSON::XS::is_bool;

my $config //= do {
    MetaCPAN::Config->new(
        name => __PACKAGE__,
        path => path(__FILE__)->parent(3)->stringify,
    );
};
$config->init_logger;

sub config () { $config->config(); }

sub es_config ( $node = undef ) {
    my $config     = config;
    my $es_servers = $config->{elasticsearch_servers};

    my $config_node
        = $node                         ? $node
        : $ENV{METACPAN_INGEST_ES_PROD} ? $es_servers->{production_node}
        : is_dev()                      ? $es_servers->{test_node}
        :                                 $es_servers->{node};

    $config_node or die "Cannot create an ES instance without a node\n";

    return {
        nodes  => $config_node,
        client => $es_servers->{client},
    };
}

sub are_you_sure ( $msg, $force = 0 ) {
    return 1 if $force;

    my $iconfirmed = 0;

    if ( -t *STDOUT ) {
        my $answer
            = prompt colored( ['bold red'], "*** Warning ***: $msg" ) . "\n"
            . 'Are you sure you want to do this (type "YES" to confirm) ? ';
        if ( $answer ne 'YES' ) {
            log_error {"Confirmation incorrect: '$answer'"};
            print "Operation will be interruped!\n";

            # System Error: 125 - ECANCELED - Operation canceled
            handle_error( 125, 'Operation canceled on User Request', 1 );
        }
        else {
            log_info {'Operation confirmed.'};
            print "alright then...\n";
            $iconfirmed = 1;
        }
    }
    else {
        log_info {"*** Warning ***: $msg"};
        $iconfirmed = 1;
    }

    return $iconfirmed;
}

sub author_dir ($pauseid) {
    my $dir = 'id/'
        . sprintf( "%s/%s/%s",
        substr( $pauseid, 0, 1 ),
        substr( $pauseid, 0, 2 ), $pauseid );
    return $dir;
}

sub cpan_dir () {
    my $config = config();
    my $cpan   = $ENV{INGEST_TEST} ? $config->{cpan_test} : $config->{cpan};

    return path($cpan) if -d $cpan;
    die
        "Couldn't find a local cpan mirror. Please specify --cpan or set MINICPAN";
}

sub diff_struct ( $old_root, $new_root, $allow_extra ) {
    my (@queue) = [ $old_root, $new_root, '', $allow_extra ];

    while ( my $check = shift @queue ) {
        my ( $old, $new, $path, $allow_extra ) = @$check;
        if ( !defined $new ) {
            return [ $path, $old, $new ]
                if defined $old;
        }
        elsif ( !is_ref($new) ) {
            return [ $path, $old, $new ]
                if !defined $old
                or is_ref($old)
                or $new ne $old;
        }
        elsif ( ref($new) eq 'SCALAR'
            || ( blessed($new) && $new->isa('JSON::PP::Boolean') ) )
        {
            my $n = ref($new) eq 'SCALAR' ? !!$$new : !!$new;
            my $o
                = !defined($old)        ? undef
                : ref($old) eq 'SCALAR' ? !!$$old
                : blessed($old)         ? !!$old
                :                         !!$old;
            return [ $path, $old, $new ] if !defined $old or $n != $o;
        }
        elsif ( is_plain_arrayref($new) ) {
            return [ $path, $old, $new ]
                if !is_plain_arrayref($old) || @$new != @$old;
            push @queue, map [ $old->[$_], $new->[$_], "$path/$_" ],
                0 .. $#$new;
        }
        elsif ( is_plain_hashref($new) ) {
            return [ $path, $old, $new ]
                if !is_plain_hashref($old)
                || !$allow_extra && keys %$new != keys %$old;
            push @queue, map [ $old->{$_}, $new->{$_}, "$path/$_" ],
                keys %$new;
        }
        else {
            die "can't compare $new type data at $path";
        }
    }
    return undef;
}

sub tmp_dir ( $cpanid, $distfile ) {
    my $dir = path('/tmp');
    return $dir->child( author_dir($cpanid), $distfile );
}

sub digest (@params) {
    my $digest = join( "\0", @params );
    $digest = Digest::SHA::sha1_base64($digest);
    $digest =~ tr{+/}{-_};
    return $digest;
}

sub fix_version ($version) {
    return 0 unless defined $version;
    my $v = ( $version =~ s/^v//i );
    $version =~ s/[^\d\._].*//;
    $version =~ s/\.[._]+/./;
    $version =~ s/[._]*_[._]*/_/g;
    $version =~ s/\.{2,}/./g;
    $v ||= $version =~ tr/.// > 1;
    $version ||= 0;
    return ( ( $v ? 'v' : '' ) . $version );
}

sub handle_error ( $exit_code, $error, $die_always = 0 ) {

    # Always log.
    log_fatal {$error};

    $! = $exit_code;

    Carp::croak $error if $die_always;
}

sub home () {
    my $dir = Cwd::abs_path( File::Spec->catdir(
        File::Basename::dirname(__FILE__),
        ( File::Spec->updir ) x 2
    ) );

    my $path = path($dir);
    die "Failed to find git dir: '$path'" unless $path;
    return $path;
}

# TODO: there must be a better way
sub is_dev () {
    return ( $ENV{PLACK_ENV} && $ENV{PLACK_ENV} =~ /dev/ );
}

sub minion () {
    require 'Mojo::Server';
    return Mojo::Server->new->build_app('MetaCPAN::API')->minion;
}

sub numify_version ($version) {
    $version = fix_version($version);
    $version =~ s/_//g;
    if ( $version =~ s/^v//i || $version =~ tr/.// > 1 ) {
        my @parts = split /\./, $version;
        my $n     = shift @parts;
        return 0 unless defined $n;
        $version
            = sprintf( join( '.', '%s', ( '%03s' x @parts ) ), $n, @parts );
    }
    $version += 0;
    return $version;
}

sub ua ( $proxy = undef ) {
    my $ua = LWP::UserAgent->new;

    if ($proxy) {
        $proxy eq 'env'
            ? $ua->env_proxy
            : $ua->proxy( [qw< http https >], $proxy );
    }

    $ua->agent('MetaCPAN');

    return $ua;
}

sub read_url ($url) {
    my $ua   = ua();
    my $resp = $ua->get($url);

    handle_error( 1, $resp->status_line, 1 ) unless $resp->is_success;

    # clean up headers if .json.gz is served as gzip type
    # rather than json encoded with gzip
    if ( $resp->header('Content-Type') eq 'application/x-gzip' ) {
        $resp->header( 'Content-Type'     => 'application/json' );
        $resp->header( 'Content-Encoding' => 'gzip' );
    }

    return $resp->decoded_content;
}

sub cpan_file_map ( $ls = undef ) {
    if ( !$ls ) {
        my $cpan = cpan_dir();
        $ls = $cpan->child(qw< indices find-ls.gz >);
        if ( !-e $ls ) {
            die "File $ls does not exist";
        }
    }

    log_info {"Reading $ls"};

    my $ret = {};

    open my $fh, "<:gzip", $ls;
    while (<$fh>) {
        my $path = ( split(/\s+/) )[-1];
        next unless ( $path =~ /^authors\/id\/\w+\/\w+\/(\w+)\/(.*)$/ );
        $ret->{$1}{$2} = 1;
    }
    close $fh;

    return $ret;
}

sub download_url ( $pauseid, $archive ) {
    return sprintf( "%s/%s/%s",
        'https://cpan.metacpan.org/authors',
        author_dir($pauseid), $archive );
}

# TODO: E<escape>
sub strip_pod ($pod) {
    $pod =~ s/L<([^\/]*?)\/([^\/]*?)>/$2 in $1/g;
    $pod =~ s/\w<(.*?)(\|.*?)?>/$1/g;
    return $pod;
}

sub extract_section ( $pod, $section ) {
    eval { $pod = decode_utf8( $pod, Encode::FB_CROAK ) };
    return undef
        unless ( $pod =~ /^=head1\s+$section\b(.*?)(^((\=head1)|(\=cut)))/msi
        || $pod =~ /^=head1\s+$section\b(.*)/msi );
    my $out = $1;
    $out =~ s/^\s*//g;
    $out =~ s/\s*$//g;
    return $out;
}

sub read_00whois ( $file = undef ) {
    my $cpan = cpan_dir();
    my $authors_file
        = $file || sprintf( "%s/%s", $cpan, 'authors/00whois.xml' );

    my $data = XMLin(
        $authors_file,
        ForceArray    => 1,
        SuppressEmpty => '',
        NoAttr        => 1,
        KeyAttr       => [],
    );

    my $whois_data = {};

    for my $author ( @{ $data->{cpanid} } ) {
        my $data = {
            map {
                my $content = $author->{$_};
                @$content == 1
                    && !ref $content->[0] ? ( $_ => $content->[0] ) : ();
            } keys %$author
        };

        my $id       = $data->{id};
        my $existing = $whois_data->{$id};
        if (  !$existing
            || $existing->{type} eq 'author' && $data->{type} eq 'list' )
        {
            $whois_data->{$id} = $data;
        }
    }

    return $whois_data;
}

# TODO: replace usage with read_02packages
sub read_02packages_fh (%args) {
    my $log_meta = $args{log_meta} // 0;
    my $file     = $args{file};

    my $fh;
    if ($file) {
        $fh = path($file)->openr(':gzip');
    }
    else {
        my $cpan = cpan_dir();
        $fh = $cpan->child(qw< modules 02packages.details.txt.gz >)
            ->openr(':gzip');
    }

    # read first 9 lines (meta info)
    my $meta = "Meta info:\n";
    for ( 0 .. 8 ) {
        chomp( my $line = <$fh> );
        next unless $line;
        $meta .= "$line\n";
    }
    log_debug {$meta} if $log_meta;

    return $fh;
}

sub read_02packages ( $file = undef ) {
    my $content;
    if ($file) {
        $content = path($file)->stringify;
    }
    else {
        my $cpan = cpan_dir();
        $content = $cpan->child(qw< modules 02packages.details.txt.gz >)
            ->stringify;
    }

    return Parse::CPAN::Packages::Fast->new($content);
}

# TODO: replace usage with unified read_06perms
sub read_06perms_fh ( $file = undef ) {
    return path($file)->openr if $file;

    my $cpan = cpan_dir();
    return $cpan->child(qw< modules 06perms.txt >)->openr;
}

sub read_06perms_iter ( $file = undef ) {
    my $file_path;
    if ($file) {
        $file_path = path($file)->absolute;
    }
    else {
        my $cpan = cpan_dir();
        $file_path = $cpan->child(qw< modules 06perms.txt >)->absolute;
    }

    my $pp = PAUSE::Permissions->new( path => $file_path );
    return $pp->module_iterator;
}

sub read_recent_segment ($segment) {
    my $cpan = cpan_dir();
    return $cpan->child("RECENT-$segment.json")->slurp;
}

1;

__END__

=head1 NAME

MetaCPAN::Ingest - Shared utilities for MetaCPAN ingestion scripts

=head1 SYNOPSIS

    use MetaCPAN::Ingest qw( config cpan_dir read_02packages );

=head1 DESCRIPTION

Provides configuration loading, CPAN file I/O, version normalisation, and
miscellaneous helpers used across ingestion scripts and modules. All functions
are exportable via L<Sub::Exporter>.

=head1 FUNCTIONS

=head2 config

Returns the loaded configuration hashref from C<metacpan_ingest.yaml>.

=head2 es_config

    my $cfg = es_config( $node );

Returns a hashref for L<Search::Elasticsearch>. Selects the production node
when C<METACPAN_INGEST_ES_PROD> is set, the test node when C<PLACK_ENV=dev>,
otherwise the default node.

=head2 are_you_sure

    are_you_sure( $message, $force );

Prompts for C<YES> confirmation on a terminal. Skips the prompt when C<$force>
is true or when stdout is not a TTY.

=head2 author_dir

    my $path = author_dir('PAUSEID');

Returns the relative CPAN author path, e.g. C<id/P/PA/PAUSEID>.

=head2 cpan_dir

Returns a L<Path::Tiny> path to the local CPAN mirror. Uses the C<cpan_test>
path from config when C<INGEST_TEST=1>.

=head2 cpan_file_map

    my $map = cpan_file_map( $find_ls_gz );

Parses C<indices/find-ls.gz> and returns a hashref of
C<< { author => { filename => 1 } } >>.

=head2 download_url

    my $url = download_url( $pauseid, $archive );

Constructs the canonical C<cpan.metacpan.org> download URL for a distribution.

=head2 tmp_dir

    my $dir = tmp_dir( $cpanid, $distfile );

Returns a C</tmp>-based path for temporary distribution extraction.

=head2 digest

    my $id = digest( @parts );

Returns a URL-safe base64 SHA-1 digest of the joined parts, used as an
Elasticsearch document id.

=head2 fix_version

Normalises a raw version string into a consistent form.

=head2 numify_version

Converts a version string to a numeric value suitable for range comparisons.

=head2 handle_error

    handle_error( $exit_code, $message, $die );

Logs a fatal message and optionally croaks with the given exit code.

=head2 home

Returns the project root directory as a L<Path::Tiny> path.

=head2 is_dev

Returns true when C<PLACK_ENV> contains C<dev>.

=head2 minion

Returns the L<Minion> job queue instance from the MetaCPAN API application.

=head2 ua

    my $ua = ua( $proxy );

Returns an L<LWP::UserAgent> with a C<MetaCPAN> agent string, optionally
configured with a proxy.

=head2 read_url

Fetches a URL and returns the decoded response body. Dies on HTTP errors.

=head2 strip_pod

Strips POD markup from a string, returning plain text.

=head2 extract_section

    my $text = extract_section( $pod, 'NAME' );

Extracts a named C<=head1> section from a POD string.

=head2 read_00whois

    my $data = read_00whois( $file );

Parses C<authors/00whois.xml> and returns a hashref keyed by PAUSE ID.

=head2 read_02packages

    my $packages = read_02packages( $file );

Returns a L<Parse::CPAN::Packages::Fast> object for
C<modules/02packages.details.txt.gz>.

=head2 read_02packages_fh

    my $fh = read_02packages_fh( file => $path, log_meta => 1 );

Opens C<02packages.details.txt.gz> and returns a filehandle positioned past
the 9-line header block.

=head2 read_06perms_fh

Returns a filehandle for C<modules/06perms.txt>.

=head2 read_06perms_iter

Returns a L<PAUSE::Permissions> module iterator for C<modules/06perms.txt>.

=head2 read_recent_segment

    my $json = read_recent_segment('1h');

Slurps a C<RECENT-*.json> file from the CPAN mirror.

=head2 true / false / is_bool

JSON boolean helpers re-exported from L<Cpanel::JSON::XS>.

=cut
