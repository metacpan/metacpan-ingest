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
my $d_bin      = $home->child('bin');
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
    my $findls_file   = $d_indices->child('find-ls.gz');

    # run the author indexing script
    `perl $author_script -whois_file $whois_file -findls_file $findls_file`;

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

    my $es_file    = MetaCPAN::ES->new( index => 'file' );
    my $file_count = $es_file->count(
        body => {
            query => { match => { release => 'HTML-Parser-3.83' } },
        }
    )->{count};
    ok( $file_count > 0, "Found ($file_count) files for HTML-Parser-3.83" );

    my $es_release    = MetaCPAN::ES->new( index => 'release' );
    my $release_count = $es_release->count(
        body => {
            query => { match => { name => 'HTML-Parser-3.83' } },
        }
    )->{count};

    ok( $release_count == 1,
        "Found ($release_count) release entries for HTML-Parser-3.83" );
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

subtest 'River Indexing' => sub {
    my $river_script = $d_bin->child('river.pl');
    my $river_file   = $d_test->child('river-of-cpan.json');

    # run the river indexing script
    `perl $river_script -json $river_file`;

    my $es_distribution = MetaCPAN::ES->new( index => 'distribution' );
    my $dist = $es_distribution->get( id => 'HTML-Parser' );
    ok( exists $dist->{_source}{river}, "Found River entry" );
};

subtest 'Contributor Indexing' => sub {
    my $contributor_script = $d_bin->child('contributor.pl');

    # run the contributor indexing script
    `perl $contributor_script -release OALDERS/HTML-Parser-3.83`;

    my $es_contributor    = MetaCPAN::ES->new( index => 'contributor' );
    my $contributor_count = $es_contributor->count(
        body => {
            query => { match => { release_name => 'HTML-Parser-3.83' } },
        }
    )->{count};
    ok( $contributor_count > 0,
        "Found ($contributor_count) contributors for HTML-Parser-3.83" );
};

subtest 'CVE Indexing' => sub {
    my $cve_script = $d_bin->child('cve.pl');
    my $json       = $d_test->child('cve_dev.json');

    # run the CVE indexing script
    `perl $cve_script -json $json`;

    my $es_cve    = MetaCPAN::ES->new( index => 'cve' );
    my $cve_count = $es_cve->count(
        body => {
            query => { match => { distribution => 'HTML-Parser' } },
        }
    )->{count};
    ok( $cve_count > 0,
        "Found ($cve_count) test CVEs" );
};

# TODO:
# favorite

# $server->set_latest;
# $server->set_first;
# $server->prepare_user_test_data;

done_testing;
