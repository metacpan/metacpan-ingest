package MetaCPAN::External::Debian;

use strict;
use warnings;
use v5.36;

use CPAN::DistnameInfo ();
use DBI                ();

use MetaCPAN::ES;

use Sub::Exporter -setup => {
    exports => [ qw<
        run_debian
    > ]
};

sub run_debian () {
    my $ret = {};

    my $host_regex = _get_host_regex();

    # connect to the database
    my $dbh = DBI->connect( "dbi:Pg:host=udd-mirror.debian.net;dbname=udd",
        'udd-mirror', 'udd-mirror' );

    # special cases
    my %skip = ( 'libbssolv-perl' => 1 );

    # multiple queries are needed
    my @sql = (

        # packages with upstream identified as CPAN
        q{select u.source, u.upstream_url from upstream_metadata um join upstream u on um.source = u.source where um.key='Archive' and um.value='CPAN'},

        # packages which upstream URL pointing to CPAN
        qq{select source, upstream_url from upstream where upstream_url ~ '${\$host_regex}'},
    );

    my @failures;

    for my $sql (@sql) {
        my $sth = $dbh->prepare($sql);
        $sth->execute();

        # map Debian source package to CPAN distro
        while ( my ( $source, $url ) = $sth->fetchrow ) {
            next if $skip{$source};
            if ( my $dist = dist_for_debian( $source, $url ) ) {
                $ret->{dist}{$dist} = $source;
            }
            else {
                push @failures => [ $source, $url ];
            }
        }
    }

    if (@failures) {
        my $ret->{errors_email_body} = join "\n" =>
            map { sprintf "%s %s", $_->[0], $_->[1] // '<undef>' } @failures;
    }

    return $ret;
}

sub dist_for_debian ( $source, $url ) {
    my %alias = (
        'datapager'   => 'data-pager',
        'html-format' => 'html-formatter',
    );

    my $dist = CPAN::DistnameInfo->new($url);
    if ( $dist->dist ) {
        return $dist->dist;
    }
    elsif ( $source =~ /^lib(.*)-perl$/ ) {
        my $es  = MetaCPAN::ES->new( index => 'release' );
        my $res = $es->scroll(
            body => {
                query => {
                    term => { 'distribution.lowercase' => $alias{$1} // $1 }
                },
                sort => [ { 'date' => 'desc' } ],
            }
        )->next;

        return $res->{_source}{distribution}
            if $res;
    }

    return;
}

sub _get_host_regex () {
    my @cpan_hosts = qw<
        backpan.cpan.org
        backpan.perl.org
        cpan.metacpan.org
        cpan.noris.de
        cpan.org
        cpan.perl.org
        search.cpan.org
        www.cpan.org
        www.perl.com
    >;

    return
        '^(https?|ftp)://('
        . join( '|', map {s/\./\\./r} @cpan_hosts ) . ')/';
}

1;
