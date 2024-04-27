use strict;
use warnings;
use v5.36;

use CPAN::DistnameInfo ();
use Getopt::Long;
use IO::Uncompress::Gunzip ();
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    config
    cpan_dir
>;

# args
my $cleanup;
GetOptions( "cleanup" => \$cleanup );

# setup
my $config = config();
$config->init_logger;

my $cpan = cpan_dir();
my $es   = MetaCPAN::ES->new( type => "package" );
my $bulk = $es->bulk();

log_info {'Reading 02packages.details'};

my $fh = _get_02packages_fh();

# read first 9 lines (meta info)
my $meta = "Meta info:\n";
for ( 0 .. 8 ) {
    chomp( my $line = <$fh> );
    next unless $line;
    $meta .= "$line\n";
}
log_debug {$meta};

my %seen;
log_debug {"adding data"};

# read the rest of the file line-by-line (too big to slurp)
while ( my $line = <$fh> ) {
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

### TODO: fix reading
sub _get_02packages_fh () {
    my $file
        = $cpan->child(qw< modules 02packages.details.txt.gz >)->stringify;
    return IO::Uncompress::Gunzip->new($file);
}

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
    $bulk->flush;
}

1;

__END__
