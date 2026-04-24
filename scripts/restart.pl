use strict;
use warnings;
use v5.36;

use MetaCPAN::Logger qw( :log :dlog );

use MetaCPAN::ES;

# setup
my $es = MetaCPAN::ES->new();

$es->restart( delay => '5s' );

log_info {'Done'};

1;

__END__

=pod

=head1 NAME

restart - Request an Elasticsearch cluster rolling restart

=head1 SYNOPSIS

 # scripts/restart

=head1 DESCRIPTION

Requests a rolling restart of the Elasticsearch cluster with a 5-second delay
between node restarts.

=cut
