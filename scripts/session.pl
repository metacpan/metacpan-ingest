use strict;
use warnings;
use v5.36;

use DateTime ();

use MetaCPAN::Logger qw( :log :dlog );

use MetaCPAN::ES;

# setup
my $es     = MetaCPAN::ES->new( index => "session" );
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
$es->index_refresh;

log_info {'done'};

1;

=pod

=head1 NAME

session - Purge expired MetaCPAN user sessions from Elasticsearch

=head1 SYNOPSIS

 # scripts/session

=head1 DESCRIPTION

Deletes user session documents from the C<session> index that have not been
updated in the past month. Iterates via scroll and performs a bulk delete.

=cut
