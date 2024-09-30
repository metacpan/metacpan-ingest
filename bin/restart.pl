use strict;
use warnings;
use v5.36;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;

# setup
my $es = MetaCPAN::ES->new();

$es->restart( delay => '5s' );

log_info {'Done'};

1;

__END__

=pod

=head1 SYNOPSIS

 # bin/restart

=head1 DESCRIPTION

kick the ES cluster

=cut
