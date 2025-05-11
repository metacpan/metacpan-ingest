use strict;
use warnings;
use v5.36;

use feature qw< state >;
use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;
use Cpanel::JSON::XS qw< decode_json encode_json >;
use DateTime         ();
use IO::Zlib         ();
use Path::Tiny       qw< path >;
use Try::Tiny        qw< catch try >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< home true >;

# config

# args
my $batch_size = 100;
my $size       = 1000;
my $index      = "cpan";

my ( $dry_run, $purge, $restore );
GetOptions(
    "batch_size=i" => \$batch_size,
    "dry_run"      => \$dry_run,
    "index=s"      => \$index,
    "purge"        => \$purge,
    "restore=s"    => \$restore,
    "size=i"       => \$size,
);

# setup
my $home = home();

run_restore() if $restore;
run_purge()   if $purge;
run_backup() unless $restore or $purge;

1;

###

sub run_restore () {
    my $restore_path;
    $restore_path = path($restore);
    $restore_path->exists or die "$restore doesn't exist\n";

    log_info { 'Restoring from ', $restore_path };

    my @bulk;

    my $fh = IO::Zlib->new( $restore_path->stringify, 'rb' );

    my %es_store;
    my %bulk_store;

    while ( my $line = $fh->readline ) {

        state $line_count = 0;
        ++$line_count;
        my $raw;

        try { $raw = decode_json($line) }
        catch {
            log_warn {"cannot decode JSON: $line --- $&"};
        };

        # Create our bulk_helper if we need,
        # incase a backup has mixed _index
        # create a new bulk helper for each
        my $key = $raw->{_index};

        $es_store{$key} ||= MetaCPAN::ES->new( index => $key );
        my $es = $es_store{$key};

        $bulk_store{$key} ||= $es->bulk( max_count => $batch_size );
        my $bulk = $bulk_store{$key};

        my $parent = $raw->{_parent};

        if ( $key eq 'author' ) {

            # Hack for dodgy lat / lon's
            if ( my $loc = $raw->{_source}{location} ) {

                my $lat = $loc->[1];
                my $lon = $loc->[0];

                if ( $lat > 90 or $lat < -90 ) {

                    # Invalid latitude
                    delete $raw->{_source}{location};
                }
                elsif ( $lon > 180 or $lon < -180 ) {

                    # Invalid longitude
                    delete $raw->{_source}{location};
                }
            }
        }

        if ( $es->exists( id => $raw->{_id} ) ) {
            $bulk->update( {
                id            => $raw->{_id},
                doc           => $raw->{_source},
                doc_as_upsert => 1,
            } );
        }
        else {
            $bulk->create( {
                id => $raw->{_id},
                $parent ? ( parent => $parent ) : (),
                source => $raw->{_source},
            } );
        }
    }

    # Flush anything left over just incase
    $_->index_refresh for values %es_store;
    $_->flush         for values %bulk_store;

    log_info {'done'};
}

sub run_purge () {
    my $now    = DateTime->now;
    my $backup = $home->child(qw< var backup >);

    $backup->visit(
        sub {
            my $file = shift;
            return if $file->is_dir;

            my $mtime = DateTime->from_epoch( epoch => $file->stat->mtime );

            # keep a daily backup for one week
            return if $mtime > $now->clone->subtract( days => 7 );

            # after that keep weekly backups
            if ( $mtime->clone->truncate( to => 'week' )
                != $mtime->clone->truncate( to => 'day' ) )
            {
                log_info        {"Removing old backup $file"};
                return log_info {'Not (dry run)'} if $dry_run;
                $file->remove;
            }
        },
        { recurse => 1 }
    );
}

sub run_backup {
    my $filename
        = join( '-', DateTime->now->strftime('%F'), grep {defined} $index );

    my $file = $home->child( qw< var backup >, "$filename.json.gz" );
    $file->parent->mkpath unless ( -e $file->parent );
    my $fh = IO::Zlib->new( "$file", 'wb4' );

    my $es     = MetaCPAN::ES->new( index => $index );
    my $scroll = $es->scroll(
        scroll => '1m',
        body   => {
            _source => true,
            size    => $size,
            sort    => '_doc',
        },
    );

    log_info { 'Backing up ', $scroll->total, ' documents' };

    while ( my $result = $scroll->next ) {
        print $fh encode_json($result), $/;
    }

    close $fh;
    log_info {'done'};
}

__END__

=head1 NAME

Backup indices

=head1 SYNOPSIS

 $ bin/backup --index author

 $ bin/backup --purge

 $ bin/backup --restore path

=head1 DESCRIPTION

Creates C<.json.gz> files in C<var/backup>. These files contain
one record per line.

=head2 purge

Purges old backups. Backups from the current week are kept.
