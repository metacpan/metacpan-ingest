use strict;
use warnings;
use v5.36;

use DateTime ();

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;

# setup
my $es     = MetaCPAN::ES->new( index => "user", type => "session" );
my $bulk   = $es->bulk( max_count => 10_000, );
my $scroll = $es->scroll(
    size   => 10_000,
    scroll => '1m',
);

my $cutoff = DateTime->now->subtract( months => 1 )->epoch;

while ( my $search = $scroll->next ) {
    next unless $search->{_source}{__updated} < $cutoff;
    $bulk->delete( { id => $search->{_id} } );
}

$bulk->flush;

log_info {'done'};

1;

=pod

Purges user sessions. we iterate over the sessions for the time being and
perform bulk delete.

=cut
