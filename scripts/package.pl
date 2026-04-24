use strict;
use warnings;
use v5.36;

use Getopt::Long;
use CPAN::DistnameInfo ();

use MetaCPAN::Logger qw( :log :dlog );

use MetaCPAN::ES;
use MetaCPAN::Ingest qw( fix_version read_02packages_fh true );

# args
my ( $cleanup, $package_file );
GetOptions(
    "cleanup"        => \$cleanup,
    "package_file=s" => \$package_file,
);

# setup
my $es   = MetaCPAN::ES->new( index => "package" );
my $bulk = $es->bulk();

my %seen;
log_debug {"adding data"};

log_info {'Reading 02packages.details'};

# read the rest of the file line-by-line (too big to slurp)

my $fh_packages = read_02packages_fh(
    log_meta => 1,
    ( $package_file ? ( file => $package_file ) : () )
);
while ( my $line = <$fh_packages> ) {
    next unless $line;
    chomp($line);

    my ( $name, $version, $file ) = split /\s+/ => $line;
    my $distinfo = CPAN::DistnameInfo->new($file);

    my $doc = +{
        module_name  => $name,
        version      => $version,
        file         => $file,
        author       => $distinfo->cpanid,
        distribution => $distinfo->dist,
        dist_version => fix_version( $distinfo->version ),
    };

    $bulk->update( {
        id            => $name,
        doc           => $doc,
        doc_as_upsert => true,
    } );

    $seen{$name} = 1;
}

$bulk->flush;

run_cleanup( \%seen ) if $cleanup;

log_info {'done indexing 02packages.details'};

$es->index_refresh();

# subs

sub run_cleanup ($seen) {
    log_debug {"checking package data to remove"};

    my $scroll = $es->scroll();

    my @remove;
    my $count = $scroll->total;
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

package - Index CPAN package data from 02packages.details.txt

=head1 SYNOPSIS

 # scripts/package
 # scripts/package --cleanup
 # scripts/package --package_file /path/to/02packages.details.txt.gz

=head1 DESCRIPTION

Reads C<modules/02packages.details.txt.gz> line by line and upserts records
into the Elasticsearch C<package> index. With C<--cleanup>, removes package
records that are no longer present in the packages file.

=head1 OPTIONS

=head2 --cleanup

Remove stale package records from the index after processing.

=head2 --package_file

Path to an alternative C<02packages.details.txt.gz> file.

=cut
