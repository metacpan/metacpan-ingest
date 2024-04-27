package MetaCPAN::Ingest;

use strict;
use warnings;
use v5.36;

use Path::Tiny qw< path >;
use Ref::Util qw< is_ref is_plain_arrayref is_plain_hashref >;
use LWP::UserAgent;
use MetaCPAN::Config;
use MetaCPAN::Logger qw< :log :dlog >;

use Sub::Exporter -setup => {
    exports => [ qw<
        config
        author_dir
        cpan_dir
        cpan_file_map
        diff_struct
        minion
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

sub config () { $config }

sub author_dir ($pauseid) {
    my $dir     = 'id/'
        . sprintf( "%s/%s/%s",
        substr( $pauseid, 0, 1 ),
        substr( $pauseid, 0, 2 ), $pauseid );
    return $dir;
}

sub cpan_dir () {
    my $config = config();
    my $cpan = $config->config->{cpan};

    return path($cpan) if -d $cpan;
    die
        "Couldn't find a local cpan mirror. Please specify --cpan or set MINICPAN";
}

sub diff_struct ($old_root, $new_root, $allow_extra) {
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

sub minion () {
    require 'Mojo::Server';
    return Mojo::Server->new->build_app('MetaCPAN::API')->minion;
}

sub ua ($proxy=undef) {
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
    my $ls = $cpan->child(qw< indices find-ls.gz >);
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

1;
