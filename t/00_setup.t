use strict;
use warnings;
use lib 't/lib';

use Test::More 0.96;

use MetaCPAN::ES;
use MetaCPAN::Mapper;
use MetaCPAN::Ingest qw< home >;

$ENV{INGEST_TEST} = 1;

my $es = MetaCPAN::ES->new();
ok( $es->test, 'es is valid' );

my $mapper = MetaCPAN::Mapper->new();
ok( $mapper->test, 'mapper is valid' );

my $home       = home();
my $d_scripts  = $home->child('scripts');
my $d_test     = $home->child('test_data');
my $d_fakecpan = $d_test->child('fakecpan');
my $d_authors  = $d_fakecpan->child('authors');
my $d_modules  = $d_fakecpan->child('modules');
my $d_indices  = $d_fakecpan->child('indices');

# === Files

subtest 'Check Files' => sub {
    ok( $d_authors->child('00whois.xml'), "Found 00whois.xml" );
    ok( $d_modules->child('02packages.details.txt.gz'),
        "Found 02packages.details.txt.gz" );
    ok( $d_modules->child('06perms.txt'), "Found 06perms.txt" );
    ok( $d_indices->child('find-ls.gz'),  "Found find-ls.gz" );
};

# === test data

my $t_author        = 'OALDERS';
my $t_distribution  = 'HTML-Parser';
my $t_distribution2 = 'LWP';           # packages/permissions tests
my $t_version       = '3.83';

my $t_release      = $t_distribution . '-' . $t_version;
my $t_full_release = $t_author . '/' . $t_release;
my $t_file         = $t_release . ".tar.gz";
my $t_dir          = join "/",
    "id",
    map { substr $t_author, 0, $_ } 1, 2, length($t_author);

my $t_full_path = sprintf "%s/%s.tar.gz", $t_dir, $t_release;

subtest 'Check Packages' => sub {
    ok( $d_authors->child($t_full_path), "Found " . $t_file );
};

# === Mapping

my @indices = qw<
    author
    contributor
    cover
    cve
    distribution
    favorite
    file
    mirror
    package
    permission
    release
>;

for my $i (@indices) {
    $mapper->index_create( index => $i, add_mapping => 1, delete_first => 1 );
}

subtest 'Check Index' => sub {
    for my $i (@indices) {
        ok( $mapper->index_exists($i), "Index '$i' exists" );
    }
};

# === Index

subtest 'Author Indexing' => sub {
    my $author_script = $d_scripts->child('author.pl');
    my $whois_file    = $d_authors->child('00whois.xml');
    my $findls_file   = $d_indices->child('find-ls.gz');

    # run the author indexing script
    my $script_cmd = "perl $author_script -whois_file $whois_file -findls_file $findls_file";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_author = MetaCPAN::ES->new( index => 'author' );
    ok( $es_author->exists( index => 'author', id => $t_author ),
        "Found author $t_author" );
};

subtest 'Package Indexing' => sub {
    my $package_script = $d_scripts->child('package.pl');
    my $package_file   = $d_modules->child('02packages.details.txt.gz');

    # run the package indexing script
    my $script_cmd = "perl $package_script -package_file $package_file";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_package = MetaCPAN::ES->new( index => 'package' );
    ok( $es_package->exists( index => 'package', id => $t_distribution2 ),
        "Found package $t_distribution2" );
};

subtest 'Permissions Indexing' => sub {
    my $perms_script = $d_scripts->child('permission.pl');
    my $perms_file   = $d_modules->child('06perms.txt');

    # run the permission indexing script
    my $script_cmd = "perl $perms_script -perms_file $perms_file";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_permission = MetaCPAN::ES->new( index => 'permission' );
    ok(
        $es_permission->exists(
            index => 'permission',
            id    => $t_distribution2
        ),
        "Found permissions for $t_distribution2"
    );
};

subtest 'Release Indexing' => sub {
    my $release_script = $d_scripts->child('release.pl');
    my $release_file   = $d_authors->child($t_full_path);

    # run the release indexing script for a tarball
    my $script_cmd = "perl $release_script $release_file";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_file    = MetaCPAN::ES->new( index => 'file' );
    my $file_count = $es_file->count(
        body => {
            query => { match => { release => $t_release } },
        }
    )->{count};
    ok( $file_count > 0, "Found ($file_count) files for $t_release" );

    my $es_release    = MetaCPAN::ES->new( index => 'release' );
    my $release_count = $es_release->count(
        body => {
            query => { match => { name => $t_release } },
        }
    )->{count};

    ok( $release_count == 1,
        "Found ($release_count) release entries for $t_release" );
};

subtest 'Cover Indexing' => sub {
    my $cover_script = $d_scripts->child('cover.pl');
    my $cover_file   = $d_test->child('cpancover_dev.json');

    # run the cover indexing script
    my $script_cmd = "perl $cover_script -json $cover_file";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_cover = MetaCPAN::ES->new( index => 'cover' );
    ok( $es_cover->exists( index => 'cover', id => $t_release ),
        "Found cover data for $t_release" );
};

subtest 'River Indexing' => sub {
    my $river_script = $d_scripts->child('river.pl');
    my $river_file   = $d_test->child('river-of-cpan.json');

    # run the river indexing script
    my $script_cmd = "perl $river_script -json $river_file";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_distribution = MetaCPAN::ES->new( index => 'distribution' );
    my $dist            = $es_distribution->get( id => $t_distribution );
    ok( exists $dist->{_source}{river}, "Found River entry" );
};

subtest 'Contributor Indexing' => sub {
    my $contributor_script = $d_scripts->child('contributor.pl');

    # run the contributor indexing script
    my $script_cmd = "perl $contributor_script -release $t_full_release";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_contributor    = MetaCPAN::ES->new( index => 'contributor' );
    my $contributor_count = $es_contributor->count(
        body => {
            query => { match => { release_name => $t_release } },
        }
    )->{count};
    ok( $contributor_count > 0,
        "Found ($contributor_count) contributors for $t_release" );
};

subtest 'CVE Indexing' => sub {
    my $cve_script = $d_scripts->child('cve.pl');
    my $json       = $d_test->child('cve_dev.json');

    # run the CVE indexing script
    my $script_cmd = "perl $cve_script -json $json";
    print STDERR "Running: $script_cmd\n";
    `$script_cmd`;

    my $es_cve    = MetaCPAN::ES->new( index => 'cve' );
    my $cve_count = $es_cve->count(
        body => {
            query => { match => { distribution => $t_distribution } },
        }
    )->{count};
    ok( $cve_count > 0, "Found ($cve_count) test CVEs" );
};

# TODO:
# favorite
# first?
# last?

done_testing;
