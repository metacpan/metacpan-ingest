use strict;
use warnings;
use v5.36;

use Getopt::Long;

use MetaCPAN::Logger qw( :log :dlog );

use MetaCPAN::ES;
use MetaCPAN::Ingest qw( read_06perms_iter true );

# args
my ( $cleanup, $perms_file );
GetOptions(
    "cleanup"      => \$cleanup,
    "perms_file=s" => \$perms_file,
);

# setup
my $es   = MetaCPAN::ES->new( index => "permission" );
my $bulk = $es->bulk();

my %seen;
log_debug {"building permission data to add"};

my $iterator = read_06perms_iter($perms_file);
while ( my $perms = $iterator->next_module ) {

    # This method does a "return sort @foo", so it can't be called in the
    # ternary since it always returns false in that context.
    # https://github.com/neilb/PAUSE-Permissions/pull/16

    my $name = $perms->name;

    my @co_maints = $perms->co_maintainers;
    my $doc       = {
        module_name => $name,
        owner       => $perms->owner,

        # empty list means no co-maintainers
        # and passing the empty arrayref will force
        # deleting existingd values in the field.
        co_maintainers => \@co_maints,
    };

    $bulk->update( {
        id            => $name,
        doc           => $doc,
        doc_as_upsert => true,
    } );

    $seen{$name} = 1;
}

$bulk->flush;
$es->index_refresh;

run_cleanup( \%seen ) if $cleanup;

log_info {'done indexing 06perms'};


sub run_cleanup ($seen) {
    log_debug {"checking permission data to remove"};

    my @remove;

    my $scroll = $es->scroll();
    my $count  = $scroll->total;

    while ( my $p = $scroll->next ) {
        my $id = $p->{_id};
        unless ( exists $seen->{$id} ) {
            push @remove, $id;
            log_debug {"removed $id"};
        }
        log_debug { $count . " left to check" } if --$count % 10000 == 0;
    }

    $bulk->delete_ids(@remove);
}

1;

__END__

=head1 NAME

permission - Index PAUSE upload permissions from 06perms.txt

=head1 SYNOPSIS

 # scripts/permission
 # scripts/permission --cleanup
 # scripts/permission --perms_file /path/to/06perms.txt

=head1 DESCRIPTION

Reads C<modules/06perms.txt> and upserts permission records (owner and
co-maintainers for each module) into the Elasticsearch C<permission> index.
With C<--cleanup>, removes permission records that are no longer present in
the file.

=head1 OPTIONS

=head2 --cleanup

Remove stale permission records from the index after processing.

=head2 --perms_file

Path to an alternative C<06perms.txt> file.

=cut
