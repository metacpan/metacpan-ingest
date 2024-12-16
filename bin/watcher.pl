use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS   qw< decode_json >;
use DateTime           ();
use CPAN::DistnameInfo ();
use FindBin            ();
use Getopt::Long;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    cpan_dir
    read_recent_segment
    true
>;

# args
my ( $backpan, $dry_run );
GetOptions(
    "backpan" => \$backpan,
    "dry_run" => \$dry_run,
);

# setup
my $cpan       = cpan_dir();
my $es_release = MetaCPAN::ES->new( type => "release" );
my $es_file    = MetaCPAN::ES->new( type => "file" );

my $latest   = 0;
my @segments = qw< 1h 6h 1d 1W 1M 1Q 1Y Z >;

while (1) {
    $latest = eval { latest_release() };
    if ($@) {
        log_error {"getting latest release failed: $@"};
        sleep(15);
        next;
    }
    my @changes = $backpan ? backpan_changes() : changes();
    while ( my $release = pop(@changes) ) {
        $release->{type} eq 'delete'
            ? reindex_release($release)
            : index_release($release);
    }
    last if $backpan;
    sleep(15);
}

1;

###

sub changes () {
    my $now     = DateTime->now->epoch;
    my $archive = $latest->archive;
    my %seen;
    my @changes;
    for my $segment (@segments) {
        log_debug {"Loading RECENT-$segment.json"};
        my $json = decode_json( read_recent_segment($segment) );
        for (
            grep {
                $_->{path}
                    =~ /^authors\/id\/.*\.(tgz|tbz|tar[\._-]gz|tar\.bz2|tar\.Z|zip|7z)$/
            } grep { $backpan ? $_->{type} eq "delete" : 1 }
            @{ $json->{recent} }
            )
        {
            my $info = CPAN::DistnameInfo->new( $_->{path} );
            my $path = $info->cpanid . "/" . $info->filename;
            my $seen = $seen{$path};
            next
                if ( $seen
                && ( $_->{type} eq $seen->{type} || $_->{type} eq 'delete' )
                );
            $seen{$path} = $_;
            if ( $_->{path} =~ /\/\Q$archive\E$/ ) {
                last;
            }
            push( @changes, $_ );
        }
        if (  !$backpan
            && $json->{meta}{minmax}{min} < $latest->date->epoch )
        {
            log_debug {"Includes latest release"};
            last;
        }
    }
    return @changes;
}

sub backpan_changes () {
    my $scroll_release = $es_release->scroll(
        scroll => '1m',
        body   => {
            query => {
                bool => {
                    must_not => [ { term => { status => 'backpan' } } ],
                },
            },
            size    => 1000,
            _source => [qw< author archive >],
        },
    );

    my @changes;
    while ( my $release = $scroll_release->next ) {
        my $data = $release->{_source};
        my $path
            = $cpan->child( 'authors',
            MetaCPAN::Util::author_dir( $data->{author} ),
            $data->{archive} );

        next if -e $path;
        log_debug {"$path not in the CPAN"};
        push( @changes, { path => $path, type => 'delete' } );
    }

    return @changes;
}

sub latest_release () {
    return undef if $backpan;

    my $scroll_release = $es_release->scroll(
        scroll => '1m',
        body   => {
            query => { match_all => {} },
            sort  => { [ date => { order => 'desc' } ] },
        }
    );

    return $scroll_release->next;
}

sub index_release ($release) {
    my $archive = $cpan->child( $release->{path} )->stringify;
    for ( my $i = 0; $i < 15; $i++ ) {
        last if -e $archive;
        log_debug {"Archive $archive does not yet exist"};
        sleep(1);
    }

    unless ( -e $archive ) {
        log_error {
            "Aborting, archive $archive not available after 15 seconds";
        };
        return;
    }

    my @run = (
        $FindBin::RealBin . "/bin",
        'release', $archive, '--latest', '--queue'
    );

    log_debug {"Running @run"};
    system(@run) unless ($dry_run);
}

sub reindex_release_first ($info) {
    my $scroll_release = $es_release->scroll(
        scroll => '1m',
        body   => {
            query => {
                bool => {
                    must => [
                        { term => { author  => $info->cpanid } },
                        { term => { archive => $info->filename } },
                    ],
                },
            },
        },
    );

    return $scroll_release->next;
}

sub reindex_release ($release) {
    my $info = CPAN::DistnameInfo->new( $release->{path} );
    $release = reindex_release_first($info);
    return unless ($release);
    log_info {"Moving $release->{_source}{name} to BackPAN"};

    my $scroll_file = $es_file->scroll( {
        scroll => '1m',
        body   => {
            query => {
                bool => {
                    must => [
                        {
                            term => {
                                release => $release->{_source}{name}
                            }
                        },
                        {
                            term => {
                                author => $release->{_source}{author}
                            }
                        },
                    ],
                },
            },
            size    => 1000,
            _source => true,
            sort    => '_doc',
        },
    } );
    return if $dry_run;

    my $bulk_release = $es_release->bulk();
    my $bulk_file    = $es_file->bulk();

    while ( my $row = $scroll_file->next ) {
        my $source = $row->{_source};
        $bulk_file->index( {
            id     => $row->{_id},
            source => {
                %$source, status => 'backpan',
            }
        } );
    }

    $bulk_release->index( {
        id     => $release->{_id},
        source => {
            %{ $release->{_source} }, status => 'backpan',
        }
    } );

    $bulk_release->flush;
    $bulk_file->flush;

    # TODO - Call Fastly to purge
    # $self->purge_cpan_distnameinfos( [$info] );
}

__END__

=pod

=head1 SYNOPSIS

 # bin/watcher

=head1 DESCRIPTION

This script requires a local CPAN mirror. It watches the RECENT-*.json
files for changes to the CPAN directory every 15 seconds. New uploads
as well as deletions are processed sequentially.

=head1 OPTIONS

=head2 --backpan

This will look for the most recent release that has been deleted.
From that point on, it will look in the RECENT files for new deletions
and process them.

L<http://friendfeed.com/cpan>

=cut
