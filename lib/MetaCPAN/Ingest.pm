package MetaCPAN::Ingest;

use strict;
use warnings;
use v5.36;

use Digest::SHA;
use LWP::UserAgent;
use Path::Tiny qw< path >;
use Ref::Util qw< is_ref is_plain_arrayref is_plain_hashref >;

use MetaCPAN::Config;
use MetaCPAN::Logger qw< :log :dlog >;

use Sub::Exporter -setup => {
    exports => [ qw<
        config
        author_dir
        cpan_dir
        cpan_file_map
        diff_struct
        digest
        download_url
        fix_version
        handle_error
        minion
        numify_version
        strip_pod
        tmp_dir
        ua
    > ]
};

my $config //= do {
    MetaCPAN::Config->new(
        name => __PACKAGE__,
        path => path(__FILE__)->parent(3)->stringify,
    );
};
$config->init_logger;

sub config () {$config}

sub author_dir ($pauseid) {
    my $dir = 'id/'
        . sprintf( "%s/%s/%s",
        substr( $pauseid, 0, 1 ),
        substr( $pauseid, 0, 2 ), $pauseid );
    return $dir;
}

sub cpan_dir () {
    my $config = config();
    my $cpan   = $config->config->{cpan};

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

sub tmp_dir (@sub_dirs) {
    my $dir = path('/tmp');
    return $dir->child(@sub_dirs);    # TODO
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

sub handle_error ( $error, $die_always ) {

    # Always log.
    log_fatal {$error};

    $! = 1;    ### $self->exit_code if ( $self->exit_code != 0 );

    Carp::croak $error if $die_always;
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

sub download_url ($pauseid, $archive) {
    return sprintf(
        "%s/%s/%s",
        'https://cpan.metacpan.org/authors',
        author_dir($pauseid),
        $archive
    );
}

# TODO: E<escape>
sub strip_pod {
    my $pod = shift;
    $pod =~ s/L<([^\/]*?)\/([^\/]*?)>/$2 in $1/g;
    $pod =~ s/\w<(.*?)(\|.*?)?>/$1/g;
    return $pod;
}

1;
