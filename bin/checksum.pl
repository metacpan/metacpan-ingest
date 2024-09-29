use strict;
use warnings;
use v5.36;

use Getopt::Long;
use Digest::file qw< digest_file_hex >;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< cpan_dir >;

# args
my $limit = 1000;
my $dry_run;
GetOptions(
    "limit=i" => \$limit,
    "dry_run" => \$dry_run,
);

# setup
my $es = MetaCPAN::ES->new( type => "release" );
my $bulk;
$bulk = $es->bulk() unless $dry_run;

log_warn {"--- DRY-RUN ---"} if $dry_run;
log_info {"Searching for releases missing checksums"};

my $scroll = $es->scroll(
    scroll => '10m',
    body   => {
        query => {
            not => {
                exists => {
                    field => "checksum_md5"
                }
            }
        }
    },
    fields => [qw< id name download_url >],
);

log_warn { "Found " . $scroll->total . " releases" };
log_warn { "Limit is " . $limit };

my $count = 0;

while ( my $p = $scroll->next ) {
    if ( $limit >= 0 and $count++ >= $limit ) {
        log_info {"Max number of changes reached."};
        last;
    }

    log_info { "Adding checksums for " . $p->{fields}{name}[0] };

    if ( my $download_url = $p->{fields}{download_url} ) {
        my $file
            = cpan_dir . "/authors" . $p->{fields}{download_url}[0]
            =~ s/^.*authors//r;
        my $checksum_md5    = digest_file_hex( $file, 'MD5' );
        my $checksum_sha256 = digest_file_hex( $file, 'SHA-256' );

        if ($dry_run) {
            log_info { "--- MD5: " . $checksum_md5 };
            log_info { "--- SHA256: " . $checksum_sha256 };
        }
        else {
            $bulk->update( {
                id  => $p->{_id},
                doc => {
                    checksum_md5    => $checksum_md5,
                    checksum_sha256 => $checksum_sha256
                },
                doc_as_upsert => 1,
            } );
        }
    }
    else {
        log_info { $p->{fields}{name}[0] . " is missing a download_url" };
    }
}

$bulk->flush unless $dry_run;

log_info {'Finished adding checksums'};

1;

__END__

=pod

=head1 SYNOPSIS

 # bin/metacpan checksum --[no-]dry_run --limit X

=head1 DESCRIPTION

Backfill checksums for releases

=head2 dry_run

Don't update - just show what would have been updated (default)

=head2 no-dry_run

Update records

=head2 limit

Max number of records to update. default=1000, for unlimited set to -1

=cut
