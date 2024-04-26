package MetaCPAN::Ingest;

use strict;
use warnings;

use Path::Tiny qw< path >;
use Ref::Util qw< is_ref is_plain_arrayref is_plain_hashref >;
use LWP::UserAgent;

use Sub::Exporter -setup => {
    exports => [
        qw<
            author_dir
            cpan_dir
            diff_struct
            ua
        >
    ]
};

sub author_dir {
    my $pauseid = shift;
    my $dir     = 'id/'
        . sprintf( "%s/%s/%s",
        substr( $pauseid, 0, 1 ),
        substr( $pauseid, 0, 2 ), $pauseid );
    return $dir;
}

sub cpan_dir {
    my @dirs = (
        $ENV{MINICPAN},    '/home/metacpan/CPAN',
        "$ENV{HOME}/CPAN", "$ENV{HOME}/minicpan",
    );
    foreach my $dir ( grep {defined} @dirs ) {
        return path($dir) if -d $dir;
    }
    die "Couldn't find a local cpan mirror. Please specify --cpan o\
r set MINICPAN";
}

sub diff_struct {
    my ( $old_root, $new_root, $allow_extra ) = @_;
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

sub ua {
    my ($proxy) = @_;

    my $ua = LWP::UserAgent->new;

    if ($proxy) {
        $proxy eq 'env'
            ? $ua->env_proxy
            : $ua->proxy( [qw< http https >], $proxy );
    }

    $ua->agent('MetaCPAN');

    return $ua;
}

1;
