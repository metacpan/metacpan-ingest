use strict;
use warnings;
use v5.36;

use CPAN::DistnameInfo ();
use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    read_02packages_fh
>;

# args
my $cleanup;
GetOptions( "cleanup" => \$cleanup );

# setup
my $es   = MetaCPAN::ES->new( type => "package" );
my $bulk = $es->bulk();

my %seen;
log_debug {"adding data"};

log_info {'Reading 02packages.details'};

# read the rest of the file line-by-line (too big to slurp)

my $fh_packages = read_02packages_fh();
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
        dist_version => $distinfo->version,
    };

    $bulk->update( {
        id            => $name,
        doc           => $doc,
        doc_as_upsert => 1,
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
