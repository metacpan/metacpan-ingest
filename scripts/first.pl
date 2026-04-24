use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< true >;

# args
my ($distribution);
GetOptions( "distribution=s" => \$distribution, );

# setup
my $es_dist    = MetaCPAN::ES->new( index => "distribution" );
my $es_release = MetaCPAN::ES->new( index => "release" );

my $query
    = $distribution
    ? { term      => { name => $distribution } }
    : { match_all => {} };

my $size
    = $distribution
    ? 1
    : 500;

my $scroll = $es_dist->scroll(
    body => {
        query => $query,
        size  => $size,
    },
);

log_info { "processing " . $scroll->total . " distributions" };

while ( my $distribution = $scroll->next ) {
    my $dist_name = $distribution->{_source}{name};

    # find the first release for the distribution
    my $first_release = $es_release->search(
        body => {
            query   => { term => { distribution => $dist_name } },
            _source => ['name'],
            sort    => [ { date => 'asc' } ],
            size    => 1,
        }
    );

    if ( !$first_release ) {
        log_warn {"No release found for $dist_name"};
        next;
    }

    my $hits     = $first_release->{hits}{hits}[0];
    my $rel_name = $hits->{_source}{name};

    # set the first flag
    $es_release->update(
        id  => $rel_name,
        doc => { first => true },
    );
}

1;

__END__

=pod

=head1 NAME

Set the C<first> bit after a full reindex

=head1 SYNOPSIS

 $ scripts/first --distribution Moose

=head1 DESCRIPTION

Setting the C<first> bit cannot be set when indexing archives in parallel,
e.g. when doing a full reindex.
This script sets the C<first> bit once all archives have been indexed.

=head1 OPTIONS

=head2 distribution

Only set the C<first> bit for releases of this distribution.

=cut
