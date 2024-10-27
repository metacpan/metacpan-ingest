use strict;
use warnings;
use v5.36;

use DBI        ();
use File::stat qw< stat >;
use Getopt::Long;
use IO::Uncompress::Bunzip2 qw< bunzip2 >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    home
    ua
>;

# args
my ( $force_refresh, $skip_download );
GetOptions(
    "force_refresh" => \$force_refresh,
    "skip_download" => \$skip_download,
);

# setup

# XXX fix hardcoded path
my $home = home();

my $db
    = $ENV{HARNESS_ACTIVE}
    ? $home->child('t/var/cpantesters-release-fake.db.bz2')
    : 'http://devel.cpantesters.org/release/release.db.bz2';

# XXX move path to config
my $mirror_file = $home->child( 'var', ( $ENV{HARNESS_ACTIVE} ? 't' : () ),
    'tmp', 'cpantesters.db' );

my $ua = ua();

my $es   = MetaCPAN::ES->new( type => "release" );
my $bulk = $es->bulk();

log_info { 'Mirroring ' . $db };

$ua->mirror( $db, "$db.bz2" ) unless $skip_download;

if ( -e $mirror_file
    && stat($mirror_file)->mtime >= stat("$mirror_file.bz2")->mtime )
{
    log_info {'DB hasn\'t been modified'};
    exit unless $force_refresh;
}

bunzip2
    "$mirror_file.bz2" => "$mirror_file",
    AutoClose          => 1
    if -e "$mirror_file.bz2";

my $scroll = $es->scroll(
    body => {
        sort => '_doc',
    },
);

my %releases;

while ( my $release = $scroll->next ) {
    my $data = $release->{_source};

    # XXX temporary hack.  This may be masking issues with release
    # versions. (Olaf)
    my $version = $data->{version};
    $version =~ s{\Av}{} if $version;

    $releases{ join( '-', grep {defined} $data->{distribution}, $version ) }
        = $data;
}

log_info { 'Opening database file at ' . $mirror_file };

my $dbh = DBI->connect( 'dbi:SQLite:dbname=' . $mirror_file );
my $sth;
$sth = $dbh->prepare('SELECT * FROM release');

$sth->execute;
my @bulk;
while ( my $row_from_db = $sth->fetchrow_hashref ) {

    # The testers db seems to return q{} where we would expect a version of
    # 0.

    my $version = $row_from_db->{version} || 0;

    # weblint++ gets a name of 'weblint' and a version of '++-1.15' from
    # the testers db.  Special case it for now.  Maybe try and get the db
    # fixed.

    $version =~ s{\+}{}g;
    $version =~ s{\A-}{};

    my $release     = join( '-', $row_from_db->{dist}, $version );
    my $release_doc = $releases{$release};

    # there's a cpantesters dist we haven't indexed
    next unless ($release_doc);

    my $insert_ok = 0;

    my $tester_results = $release_doc->{tests};
    if ( !$tester_results ) {
        $tester_results = {};
        $insert_ok      = 1;
    }

    # maybe use Data::Compare instead
    for my $condition (qw< fail pass na unknown >) {
        last if $insert_ok;
        if ( ( $tester_results->{$condition} || 0 )
            != $row_from_db->{$condition} )
        {
            $insert_ok = 1;
        }
    }

    next unless ($insert_ok);
    my %tests = map { $_ => $row_from_db->{$_} } qw< fail pass na unknown >;
    $bulk->update( {
        doc           => { tests => \%tests },
        doc_as_upsert => 1,
        id            => $release_doc->{id},
    } );
}

$bulk->flush;
$es->index_refresh;

log_info {'done'};

1;

=pod

=head1 SYNOPSIS

 $ bin/cpantesters

=head1 DESCRIPTION

Index CPAN Testers test results.

=head1 ARGUMENTS

=head2 db

Defaults to C<http://devel.cpantesters.org/release/release.db.bz2>.

=cut
