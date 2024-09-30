use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS qw< decode_json encode_json >;
use DateTime                  ();
use DateTime::Format::ISO8601 ();
use HTTP::Tiny                ();
use Getopt::Long;
use Sys::Hostname qw< hostname >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< are_you_sure >;

# setup
my $hostname        = hostname();
my $mode            = $hostname =~ /dev/ ? 'testing' : 'production';
my $bucket          = "mc-${mode}-backups";    # So we don't break production
my $repository_name = 'our_backups';

#my $es   = MetaCPAN::ES->new( type => "distribution" );
#my $bulk = $es->bulk();

# args
my (
    $date_format, $indices, $list,      $purge_old, $restore,
    $setup,       $snap,    $snap_name, $snap_stub
);
my $host = MetaCPAN::Server::Config::config()->{elasticsearch_servers};
GetOptions(
    "list"          => \$list,
    "date_format=s" => \$date_format,
    "host=s"        => \$host,
    "indices=s"     => \$indices,
    "purge_old"     => \$purge_old,
    "restore"       => \$restore,
    "setup"         => \$setup,
    "snap"          => \$snap,
    "snap_name=s"   => \$snap_name,
    "snap_stub=s"   => \$snap_stub,
);

# Note: can take wild cards https://www.elastic.co/guide/en/elasticsearch/reference/2.4/multi-index.html
$indices //= '*';

my $config = {};    ## TODO ( use MetaCPAN::Server::Config (); ??? )

my $aws_key    = $config->{es_aws_s3_access_key};
my $aws_secret = $config->{es_aws_s3_secret};

my $http_client = HTTP::Tiny->new(
    default_headers => { 'Accept' => 'application/json' },
    timeout         => 120,                                 # list can be slow
);

# run
die "es_aws_s3_access_key not in config" unless $aws_key;
die "es_aws_s3_secret not in config"     unless $aws_secret;

run_list_snaps() if $list;
run_setup()      if $setup;
run_snapshot()   if $snap;
run_purge_old()  if $purge_old;
run_restore()    if $restore;

die "setup, restore, purge-old or snap argument required";

1;

###

sub run_snapshot () {
    $snap_stub   || die 'Missing snap-stub';
    $date_format || die 'Missing date-format (e.g. %Y-%m-%d)';

    my $date      = DateTime->now->strftime($date_format);
    my $snap_name = $snap_stub . '_' . $date;

    my $data = {
        "ignore_unavailable"   => 0,
        "include_global_state" => 1,
        "indices"              => $indices,
    };

    log_debug { 'snapping: ' . $snap_name };
    log_debug { 'with indices: ' . $indices };

    my $path = "${repository_name}/${snap_name}";

    my $response = _request( 'put', $path, $data );
    return $response;
}

sub run_list_snaps () {
    my $path     = "${repository_name}/_all";
    my $response = _request( 'get', $path, {} );

    my $data = eval { decode_json $response->{content} };

    foreach my $snapshot ( @{ $data->{snapshots} || [] } ) {
        log_info { $snapshot->{snapshot} }
        Dlog_debug {$_} $snapshot;
    }

    return $response;
}

sub run_purge_old () {
    my $keep_all_after = DateTime->now->subtract( days => 30 );

    # fetch the current list
    my $path     = "${repository_name}/_all";
    my $response = _request( 'get', $path, {} );
    my $data     = eval { decode_json $response->{content} };

    my %to_delete;
    foreach my $snapshot ( @{ $data->{snapshots} || [] } ) {

        my $snap_date = DateTime::Format::ISO8601->parse_datetime(
            $snapshot->{start_time} );
        my $recent_so_keep = DateTime->compare( $snap_date, $keep_all_after );

        # keep 1st of each month
        next if $snap_date->day eq '1';

        # keep anything that is recent (as per $keep_all_after)
        next if $recent_so_keep eq '1';

        # we want to delete it then
        $to_delete{ $snapshot->{snapshot} } = 1;
    }

    foreach my $snap ( sort keys %to_delete ) {
        my $path = "${repository_name}/${snap}";
        log_info {"Deleting ${path}"};
        my $response = _request( 'delete', $path, {} );
    }
}

sub run_restore () {
    my $snap_name = $snap_name;

    are_you_sure('Restoring... will NOT rename indices as ES::Model breaks');

    # IF we were not using ES::Model!..
    # This is a safety feature, we can always
    # create aliases to point to them if required
    # just make sure there is enough disk space
    my $data = {

        #   "rename_pattern"     => '(.+)',
        #   "rename_replacement" => 'restored_$1',
    };

    # We wait until it's actually done!
    my $path     = "${repository_name}/${snap_name}/_restore";
    my $response = _request( 'post', $path, $data );
    log_info {
        'restoring: ' . $snap_name . ' - see /_cat/recovery for progress'
    }
    if $response;
    return $response;
}

sub run_setup () {
    log_debug { 'setup: ' . $repository_name };

    my $data = {
        "type"     => "s3",
        "settings" => {
            "access_key"                 => $aws_key,
            "bucket"                     => $bucket,
            "canned_acl"                 => "private",
            "max_restore_bytes_per_sec"  => '500mb',
            "max_snapshot_bytes_per_sec" => '500mb',
            "protocol"                   => "https",
            "region"                     => "us-east",
            "secret_key"                 => $aws_secret,
            "server_side_encryption"     => 1,
            "storage_class"              => "standard",
        }
    };

    my $path = "${repository_name}";

    my $response = _request( 'put', $path, $data );
    return $response;
}

sub _request ( $method, $path, $data ) {
    my $url = $host . '/_snapshot/' . $path;

    my $json = encode_json($data);

    my $response = $http_client->$method( $url, { content => $json } );

    if ( !$response->{success} && length $response->{content} ) {

        log_error { 'Problem requesting ' . $url };

        try {
            my $resp_json = decode_json( $response->{content} );
            Dlog_error {"Error response: $_"} $resp_json;
        }
        catch {
            log_error { 'Error msg: ' . $response->{content} }
        }
        return 0;
    }
    return $response;
}

__END__

=head1 NAME

MetaCPAN::Script::Snapshot - Snapshot (and restore) Elasticsearch indices

=head1 SYNOPSIS

# Setup
 $ bin/metacpan snapshot --setup (only needed once)

# Snapshot all indexes daily
 $ bin/metacpan snapshot --snap --snap-stub full --date-format %Y-%m-%d

# List what has been snapshotted
 $ bin/metacpan snapshot --list

# restore (indices are renamed from `foo` to `restored_foo`)
 $ bin/metacpan snapshot --restore --snap-name full_2016-12-01

# purge anything older than 30 days and not created on the 1st of a month
 $ bin/metacpan snapshot --purge-old

Another example..

# Snapshot just user* indexes hourly and restore
 $ bin/metacpan snapshot --snap --indices 'user*' --snap-stub user --date-format '%Y-%m-%d-%H'
 $ bin/metacpan snapshot --restore --snap-name user_2016-12-01-12

Also useful:

See status of snapshot...

 curl localhost:9200/_snapshot/our_backups/SNAP-NAME/_status

 curl localhost:9200/_cat/recovery

Add an alias to the restored index

 curl -X POST 'localhost:9200/_aliases' -d '
    {
        "actions" : [
            { "add" : { "index" : "restored_user", "alias" : "user" } }
        ]
    }'

=head1 DESCRIPTION

Tell elasticsearch to setup (only needed once), snap or
restore from backups stored in AWS S3.

You will need to run --setup on any box you wish to restore to

You will need es_aws_s3_access_key and es_aws_s3_secret setup
in your local metacpan_server_local.conf


=cut
