use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS qw< decode_json >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    home
    ua
>;

# setup
my $home = home();

my $url
    = $ENV{HARNESS_ACTIVE}
    ? 'file:' . $home->child('t/var/cpantesters-release-api-fake.json')
    : 'http://api-3.cpantesters.org/v3/release';

my $ua = ua();

my $es   = MetaCPAN::ES->new( index => "release" );
my $bulk = $es->bulk();

log_info { 'Fetching ' . $url };

my $res;
eval { $res = $ua->get($url) };
exit(1) unless $res and $res->code == 200;

my $json = $res->decoded_content;
my $data = decode_json $json;

my $scroll = $es->scroll(
    body => {
        sort => '_doc',
    },
);

# Create a cache of all releases (dist + version combos)
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

for my $row (@$data) {

    # The testers db seems to return q{} where we would expect
    # a version of 0.
    my $version = $row->{version} || 0;

    # weblint++ gets a name of 'weblint' and a version of '++-1.15'
    # from the testers db.  Special case it for now.  Maybe try and
    # get the db fixed.

    $version =~ s{\+}{}g;
    $version =~ s{\A-}{};

    my $release     = join( '-', $row->{dist}, $version );
    my $release_doc = $releases{$release};

    # there's a cpantesters dist we haven't indexed
    next unless $release_doc;

    # Check if we need to update this data
    my $insert_ok      = 0;
    my $tester_results = $release_doc->{tests};
    if ( !$tester_results ) {
        $tester_results = {};
        $insert_ok      = 1;
    }

    # maybe use Data::Compare instead
    for my $condition (qw< fail pass na unknown >) {
        last if $insert_ok;
        if ( ( $tester_results->{$condition} || 0 ) != $row->{$condition} ) {
            $insert_ok = 1;
        }
    }

    next unless $insert_ok;

    my %tests = map { $_ => $row->{$_} } qw< fail pass na unknown >;
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
