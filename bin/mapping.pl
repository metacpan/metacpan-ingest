use strict;
use warnings;

use Cpanel::JSON::XS qw< decode_json >;
use Getopt::Long;
use MetaCPAN::Mapper;
use MetaCPAN::Ingest qw< home >;

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
    my $home = home();
    my $map_file = $home->child('conf/es/' . $index . '/mapping.json');
    my $mapping = decode_json $map_file->slurp();

    $mapper->index_create($index);
    $mapper->index_put_mapping($index, $type, $mapping);
}
