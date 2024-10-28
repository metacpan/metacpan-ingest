package MetaCPAN::External::Cygwin;

use List::Util       qw< shuffle >;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::Ingest qw< ua >;

use Sub::Exporter -setup => {
    exports => [ qw<
        run_cygwin
    > ]
};

sub run_cygwin () {
    my $ret = {};

    my $ua = ua();
    my $mirrors = get_mirrors($ua);

    my @mirrors = @{ $mirrors };
    my $timeout = $ua->timeout(10);

  MIRROR: {
        my $mirror = shift @mirrors or die "Ran out of mirrors";
        log_debug {"Trying mirror: $mirror"};
        my $res = $ua->get( $mirror . 'x86_64/setup.ini' );
        redo MIRROR unless $res->is_success;

        my @packages = split /^\@ /m, $res->decoded_content;
        shift @packages;    # drop headers

        log_debug { sprintf "Got %d cygwin packages", scalar @packages };

        for my $desc (@packages) {
            next if substr( $desc, 0, 5 ) ne 'perl-';
            my ( $pkg, %attr ) = map s/\A"|"\z//gr, map s/ \z//r,
                map s/\n+/ /gr, split /^([a-z]+): /m, $desc;
            $attr{category} = [ split / /, $attr{category} ];
            next if grep /^(Debug|_obsolete)$/, @{ $attr{category} };
            $ret->{dist}{ $pkg =~ s/^perl-//r } = $pkg;
        }
    }
    $ua->timeout($timeout);

    log_debug {
        sprintf "Found %d cygwin-CPAN packages",
        scalar keys %{ $ret->{dist} }
    };

    return $ret;
}

sub _get_mirrors ( $ua ) {
    log_debug {"Fetching mirror list"};
    my $res = $ua->get('https://cygwin.com/mirrors.lst');
    die "Failed to fetch mirror list: " . $res->status_line
        unless $res->is_success;
    my @mirrors = shuffle map +( split /;/ )[0], split /\n/,
        $res->decoded_content;

    log_debug { sprintf "Got %d mirrors", scalar @mirrors };
    return \@mirrors;
}

1;
