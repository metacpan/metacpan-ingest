use strict;
use warnings;

use Getopt::Long;
use MetaCPAN::Mapper;
use MetaCPAN::Ingest qw< are_you_sure >;

my ( $index, $cmd );
GetOptions(
    "index=s" => \$index,
    "cmd=s"   => \$cmd,
);
die "cmd can only be one of: 'create', 'delete'\n"
    unless grep { $cmd eq $_ } qw< create delete >;

# setup

my $mapper = MetaCPAN::Mapper->new();

if ( $mapper->index_exists($index) ) {
    are_you_sure("This action will delete index: $index");
    $mapper->index_delete($index)
}

if ( $cmd eq 'create' ) {
    $mapper->index_create($index);
    $mapper->index_add_mapping($index);
}
