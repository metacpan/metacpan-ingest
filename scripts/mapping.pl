use strict;
use warnings;
use v5.36;

use Getopt::Long;

use MetaCPAN::Logger qw( :log :dlog );

use MetaCPAN::Mapper;
use MetaCPAN::Ingest qw( are_you_sure );

my ( $index, $cmd );
GetOptions(
    "index=s" => \$index,
    "cmd=s"   => \$cmd,
);
die "cmd can only be one of: 'create', 'delete'\n"
    unless grep { $cmd eq $_ } qw< create delete >;

# setup

my $mapper = MetaCPAN::Mapper->new();

my $all_indices = $mapper->available_mappings();

# check given index has a mapping file
# TODO: support a given mapping file
my @indices;
if ( $index eq 'all' ) {
    are_you_sure("This action will delete ALL indices");
    push @indices, keys %$all_indices;
}
elsif ( exists $all_indices->{$index} ) {
    are_you_sure("This action will delete index: $index");
    push @indices, $index;
} else {
    die "Index mapping doesn't exist ($index)\n";
}

for my $i ( @indices ) {
    if ( $mapper->index_exists($i) ) {
        $mapper->index_delete($i);
        log_info {"deleted index: $i"};
    }

    if ( $cmd eq 'create' ) {
        $mapper->index_create(
            index        => $i,
            add_mapping  => 1,
            delete_first => 1,
        );
        log_info {"created index: $i"};

        $mapper->index_add_mapping($i);
        log_info {"added mapping for index: $i"};
    }

    log_info {"done."};
}

__END__

=head1 NAME

mapping - Create or delete an Elasticsearch index and apply its mapping

=head1 SYNOPSIS

 # scripts/mapping --index release --cmd create
 # scripts/mapping --index release --cmd delete

=head1 DESCRIPTION

Creates or deletes a named Elasticsearch index. When creating, loads the
mapping from C<conf/es/$index/mapping.json> and settings from
C<conf/es/$index/settings.json>. Prompts for confirmation before deleting an
existing index.

=head1 OPTIONS

=head2 --index

Name of the Elasticsearch index to operate on (required).

=head2 --cmd

Action to perform: C<create> or C<delete> (required).

=cut
