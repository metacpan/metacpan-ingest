use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;

# args
my ($distribution);
GetOptions( "distribution=s" => \$distribution, );

# setup
my $es = MetaCPAN::ES->new( type => "distribution" );

my $query
    = $distribution
    ? { term      => { name => $distribution } }
    : { match_all => {} };

my $size
    = $distribution
    ? 1
    : 500;

my $scroll = $es->scroll(
    body => {
        query => $query,
        size  => $size,
    },
);

log_info { "processing " . $scroll->total . " distributions" };

while ( my $distribution = $scroll->next ) {
    my $release = $distribution->set_first_release;
    $release
        ? log_debug {
        "@{[ $release->name ]} by @{[ $release->author ]} was first"
        }
        : log_warn {
        "no release found for distribution @{[$distribution->name]}"
        };
}

# Everything changed - reboot the world!
# cdn_purge_all;

1;

__END__

=pod

=head1 NAME

Set the C<first> bit after a full reindex

=head1 SYNOPSIS

 $ bin/first --distribution Moose

=head1 DESCRIPTION

Setting the C<first> bit cannot be set when indexing archives in parallel,
e.g. when doing a full reindex.
This script sets the C<first> bit once all archives have been indexed.

=head1 OPTIONS

=head2 distribution

Only set the C<first> bit for releases of this distribution.

=cut
