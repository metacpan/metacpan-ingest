package MetaCPAN::Ingest;

use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS;
use Digest::SHA;
use Encode qw< decode_utf8 >;
use IO::Prompt::Tiny qw< prompt >;
use File::Basename ();
use File::Spec ();
use LWP::UserAgent;
use Path::Tiny qw< path >;
use PAUSE::Permissions ();
use Ref::Util qw< is_ref is_plain_arrayref is_plain_hashref >;
use Term::ANSIColor qw< colored >;
use XML::Simple qw< XMLin >;

use MetaCPAN::Config;
use MetaCPAN::Logger qw< :log :dlog >;

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

sub are_you_sure ( $msg, $force=0 ) {
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
            exit_code(125);
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
    my $cpan   = $config->{cpan};

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
    $digest =~ tr/[+\/]/-_/;
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

sub handle_error ( $exit_code, $error, $die_always ) {

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
    return $ENV{PLACK_ENV} =~ /dev/;
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

sub read_url ( $url ) {
    my $ua   = ua();
    my $resp = $ua->get($url);

    handle_error(1, $resp->status_line, 1 ) unless $resp->is_success;

    # clean up headers if .json.gz is served as gzip type
    # rather than json encoded with gzip
    if ( $resp->header('Content-Type') eq 'application/x-gzip' ) {
        $resp->header( 'Content-Type'     => 'application/json' );
        $resp->header( 'Content-Encoding' => 'gzip' );
    }

    return $resp->decoded_content;
}

sub cpan_file_map () {
    my $cpan = cpan_dir();
    my $ls   = $cpan->child(qw< indices find-ls.gz >);
    if ( !-e $ls ) {
        die "File $ls does not exist";
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
    my $cpan         = cpan_dir();
    my $authors_file = $file || sprintf( "%s/%s", $cpan, 'authors/00whois.xml' );

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
sub read_02packages_fh ( %args ) {
    my $log_meta = $args{log_meta} // 0;
    my $file = $args{file};

    my $fh;
    if ( $file ) {
        $fh = path($file)->openr(':gzip');
    } else {
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
    if ( $file ) {
        $content = path($file)->stringify;
    } else {
        my $cpan = cpan_dir();
        $content = $cpan->child(qw< modules 02packages.details.txt.gz >)->stringify;
    }

    return Parse::CPAN::Packages::Fast->new($content);
}

# TODO: replace usage with unified read_06perms
sub read_06perms_fh () {
    my $cpan = cpan_dir();
    return $cpan->child(qw< modules 06perms.txt >)->openr;
}

sub read_06perms_iter () {
    my $cpan      = cpan_dir();
    my $file_path = $cpan->child(qw< modules 06perms.txt >)->absolute;
    my $pp        = PAUSE::Permissions->new( path => $file_path );
    return $pp->module_iterator;
}

sub read_recent_segment ( $segment ) {
    my $cpan = cpan_dir();
    return $cpan->child("RECENT-$segment.json")->slurp;
}

1;
