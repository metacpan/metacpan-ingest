use strict;
use warnings;

use Getopt::Long;
use MetaCPAN::Mapper;

my ( $index, $cmd );
GetOptions(
    "index=s" => \$index,
    "cmd=s"   => \$cmd,
);
die "cmd can only be one of: 'create', 'delete'\n"
    unless grep { $cmd eq $_ } qw< create delete >;

# setup
my $type = $index;

my $mapper = MetaCPAN::Mapper->new();

$mapper->index_delete($index)
    if $mapper->index_exists($index);

if ( $cmd eq 'create' ) {
    $mapper->index_create($index);
    $mapper->index_add_mapping($index, $type);
}
