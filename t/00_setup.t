use strict;
use warnings;
use lib 't/lib';

use Test::More 0.96;

use MetaCPAN::ES;
use MetaCPAN::Mapper;
use MetaCPAN::Ingest qw< home >;

my $es     = MetaCPAN::ES->new();
my $mapper = MetaCPAN::Mapper->new();

my $home       = home();
my $d_bin      = $home->child('bin');
my $d_test     = $home->child('test_data');
my $d_fakecpan = $d_test->child('fakecpan');
my $d_authors  = $d_fakecpan->child('authors');
my $d_modules  = $d_fakecpan->child('modules');

# === Files

my @files = qw<
    00whois.xml
    02packages.details.txt.gz
    06perms.txt
>;

subtest 'Check Files' => sub {
    ok( $d_authors->child('00whois.xml'), "Found 00whois.xml" );
    ok( $d_modules->child('02packages.details.txt.gz'),
        "Found 02packages.details.txt.gz" );
    ok( $d_modules->child('06perms.txt'), "Found 06perms.txt" );
};

my @packages = qw<
    id/O/OA/OALDERS/HTML-Parser-3.83.tar.gz
>;

subtest 'Check Packages' => sub {
    ok( $d_authors->child('id/O/OA/OALDERS/HTML-Parser-3.83.tar.gz'),
        "Found HTML-Parser-3.83.tar.gz" );
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
    my $author_script = $d_bin->child('author.pl');
    my $whois_file    = $d_authors->child('00whois.xml');

    # run the author indexing script
    `perl $author_script -whois_file $whois_file`;

    my $es_author = MetaCPAN::ES->new( index => 'author' );
    ok( $es_author->exists( index => 'author', id => 'OALDERS' ),
        "Found author OALDERS" );
};

subtest 'Package Indexing' => sub {
    my $package_script = $d_bin->child('package.pl');
    my $package_file   = $d_modules->child('02packages.details.txt.gz');

    # run the package indexing script
    `perl $package_script -package_file $package_file`;

    my $es_package = MetaCPAN::ES->new( index => 'package' );
    ok( $es_package->exists( index => 'package', id => 'LWP' ),
        "Found package LWP" );
};

subtest 'Permissions Indexing' => sub {
    my $perms_script = $d_bin->child('permission.pl');
    my $perms_file   = $d_modules->child('06perms.txt');

    # run the permission indexing script
    `perl $perms_script -perms_file $perms_file`;

    my $es_permission = MetaCPAN::ES->new( index => 'permission' );
    ok( $es_permission->exists( index => 'permission', id => 'LWP' ),
        "Found permissions for LWP" );
};

subtest 'Release Indexing' => sub {
    my $release_script = $d_bin->child('release.pl');
    my $release_file
        = $d_authors->child('id/O/OA/OALDERS/HTML-Parser-3.83.tar.gz');

    # run the release indexing script for a tarball
    `perl $release_script $release_file`;

    my $es_file           = MetaCPAN::ES->new( index => 'file' );
    my $file_search_total = $es_file->search(
        body => {
            query => { match => { release => 'HTML-Parser-3.83' } },
        }
    )->{hits}{total};
    ok( $file_search_total > 0, "Found files for HTML-Parser-3.83" );

    my $es_release           = MetaCPAN::ES->new( index => 'release' );
    my $release_search_total = $es_release->search(
        body => {
            query => { match => { name => 'HTML-Parser-3.83' } },
        }
    )->{hits}{total};

    ok( $release_search_total == 1,
        "Found release entries for HTML-Parser-3.83" );
};

subtest 'Cover Indexing' => sub {
    my $cover_script = $d_bin->child('cover.pl');
    my $cover_file   = $d_test->child('cpancover_dev.json');

    # run the cover indexing script
    `perl $cover_script -json $cover_file`;

    my $es_cover = MetaCPAN::ES->new( index => 'cover' );
    ok( $es_cover->exists( index => 'cover', id => 'HTML-Parser-3.83' ),
        "Found cover data for HTML-Parser-3.83" );
};

# TODO:
# contributor
# cve
# favorite

# check test data directory
#       - check all distros for test are there
#       - check all other data sources are there to test all indices
# set ES object with (elasticsearch_test)
#       - check object's config

# $server->set_latest;
# $server->set_first;
# $server->prepare_user_test_data;

done_testing;
