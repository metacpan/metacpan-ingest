use strict;
use warnings;
use v5.36;

use Getopt::Long;
use Path::Tiny qw< path >;
use Path::Iterator::Rule ();

use MetaCPAN::Ingest qw< minion >;

# args
my ( $dir, $file );
GetOptions(
    "dir=s"  => \$dir,
    "file=s" => \$file,
);

die "Must provide a directory (-d) or file (-f)"
    unless $dir or $file;

if ($dir) {
    -d $dir or die "Invalid directory\n";
    $dir = path($dir);
}

# TODO: Need to allow file URLS
if ($file) {
    -f $file or die "Invalid file\n";
    $file = path($file);
}

my $minion = minion();

if ($dir) {
    my $rule = Path::Iterator::Rule->new;
    $rule->name(qr{\.(tgz|tbz|tar[\._-]gz|tar\.bz2|tar\.Z|zip|7z)\z});

    my $next = $rule->iter($dir);
    while ( defined( my $file = $next->() ) ) {
        $minion->enqueue(
            index_release => [$file],
            { attempts => 3 }
        );
    }
}

if ($file) {
    $minion->enqueue(
        index_release => [ $file->stringify ],
        { attempts => 3 }
    );
}

__END__

=head1 SYNOPSIS

    bin/queue --file https://cpan.metacpan.org/authors/id/O/OA/OALDERS/HTML-Restrict-2.2.2.tar.gz
    bin/queue --dir /home/metacpan/CPAN/
    bin/queue --dir /home/metacpan/CPAN/authors/id
    bin/queue --dir /home/metacpan/CPAN/authors/id/R/RW/RWSTAUNER
    bin/queue --file /home/metacpan/CPAN/authors/id/R/RW/RWSTAUNER/Timer-Simple-1.006.tar.gz

=cut
