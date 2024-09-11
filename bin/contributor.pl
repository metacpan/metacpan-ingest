use strict;
use warnings;
use v5.36;

use Getopt::Long;
use Ref::Util qw< is_arrayref >;

use MetaCPAN::Logger qw< :log :dlog >;
use MetaCPAN::ES;
use MetaCPAN::Contributor qw<
    get_cpan_author_contributors
    update_release_contirbutors
>;

# args
my $all = 0;
my ( $distribution, $release, $age );
GetOptions(
    "all"            => \$all,
    "distribution=s" => \$distribution,
    "release=s"      => \$release,
    "age=i"          => \$age,
);

# Setup
my $query
    = $all          ? { match_all => {} }
    : $distribution ? { term => { distribution => $distribution } }
    : $release      ? {
    bool => {
        must => [
            { term => { author => get_author($release) } },
            { term => { name   => $release } },
        ]
    }
    }
    : $age ? { range => { date => { gte => sprintf( 'now-%dd', $age ) } } }
    :   die "Error: must provide 'all' or 'distribution' or 'release' or 'age'";

my $body    = { query => $query };
my $timeout = $all ? '720m' : '5m';
my $fields  = [qw< author distribution name >];

my $es_release = MetaCPAN::ES->new( type => "release" );
my $scroll     = $es_release->scroll(
    body   => $body,
    scroll => $timeout,
    fields => $fields,
);

while ( my $r = $scroll->next ) {
    my $contrib_data = get_cpan_author_contributors(
        $r->{fields}{author}[0],
        $r->{fields}{name}[0],
        $r->{fields}{distribution}[0],
    );
    next unless is_arrayref($contrib_data);
    log_debug { 'adding release ' . $r->{fields}{name}[0] };

    update_release_contirbutors( $_, $timeout ) for @$contrib_data;
}

###

sub get_author ($release) {
    return unless $release;
    my $author = $release =~ s{/.*$}{}r;
    $author
        or die
        "Error: invalid 'release' argument (format: PAUSEID/DISTRIBUTION-VERSION)";
    return $author;
}

1;

__END__

=head1 SYNOPSIS

 # bin/contributor.pl --all
 # bin/contributor.pl --distribution Moose
 # bin/contributor.pl --release ETHER/Moose-2.1806

=head1 DESCRIPTION

Update the list of contributors (CPAN authors only) of all/matching
releases in the 'contributor' type (index).

=cut
