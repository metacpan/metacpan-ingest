use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< false minion >;

# args
my ( $age, $check_missing, $count, $distribution, $limit, $queue );
my $dry_run;
GetOptions(
    "age=i"          => \$age,
    "check_missing"  => \$check_missing,
    "count=i"        => \$count,
    "distribution=s" => \$distribution,
    "limit=i"        => \$limit,
    "queue"          => \$queue,
);

if ( $count and !$distribution ) {
    die
        "Cannot set count in a distribution search mode, this flag only applies to a single distribution. please use together with --distribution DIST";
}

if ( $check_missing and $distribution ) {
    die
        "check_missing doesn't work in filtered mode - please remove other flags";
}

index_favorites();

log_info {'done'};

1;

###

sub index_favorites () {
    my $query = { match_all => {} };
    my $age_filter;

    if ($age) {
        $age_filter
            = { range => { date => { gte => sprintf( 'now-%dm', $age ) } } };
    }

    if ($distribution) {
        $query = { term => { distribution => $distribution } };
    }
    elsif ($age) {
        my $es   = MetaCPAN::ES->new( index => "favorite" );
        my $favs = $es->scroll(
            scroll => '5m',
            body   => {
                query   => $age_filter,
                _source => [qw< distribution >],
                size    => $limit || 500,
                sort    => '_doc',
            }
        );

        my %recent_dists;

        while ( my $fav = $favs->next ) {
            my $dist = $fav->{_source}{distribution};
            $recent_dists{$dist}++ if $dist;
        }

        my @keys = keys %recent_dists;
        if (@keys) {
            $query = { terms => { distribution => \@keys } };
        }
        $es->index_refresh;
    }

    # get total fav counts for distributions

    my %dist_fav_count;

    if ($count) {
        $dist_fav_count{$distribution} = $count;
    }
    else {
        my $es   = MetaCPAN::ES->new( index => "favorite" );
        my $favs = $es->scroll(
            scroll => '30s',
            body   => {
                query   => $query,
                _source => [qw< distribution >],
                size    => 500,
                sort    => '_doc',
            },
        );

        while ( my $fav = $favs->next ) {
            my $dist = $fav->{_source}{distribution};
            $dist_fav_count{$dist}++ if $dist;
        }

        $es->index_refresh;
        log_debug {"Done counting favs for distributions"};
    }

    # Report missing distributions if requested

    if ($check_missing) {
        my %missing;
        my @age_filter;
        if ($age) {
            @age_filter = ( must => [$age_filter] );
        }

        my $es    = MetaCPAN::ES->new( index => "file" );
        my $files = $es->scroll(
            scroll => '15m',
            body   => {
                query => {
                    bool => {
                        must_not => [
                            { range => { dist_fav_count => { gte => 1 } } }
                        ],
                        @age_filter,
                    },
                },
                _source => [qw< id distribution >],
                size    => 500,
                sort    => '_doc',

            },
        );

        while ( my $file = $files->next ) {
            my $dist = $file->{_source}{distribution};
            next unless $dist;
            next if exists $missing{$dist} or exists $dist_fav_count{$dist};

            if ($queue) {
                log_debug {"Queueing: $dist"};
                my $minion = minion();

                my @count_flag;
                if ( $count or $dist_fav_count{$dist} ) {
                    @count_flag
                        = ( '--count', $count || $dist_fav_count{$dist} );
                }

                $minion->enqueue(
                    index_favorite =>
                        [ '--distribution', $dist, @count_flag ],
                    { priority => 0, attempts => 10 }
                );
            }
            else {
                log_debug {"Found missing: $dist"};
            }

            $missing{$dist} = 1;
            last if $limit and scalar( keys %missing ) >= $limit;
        }

        my $total_missing = scalar( keys %missing );
        log_debug {"Total missing: $total_missing"} unless $queue;

        $es->index_refresh;
        return;
    }

    # Update fav counts for files per distributions

    for my $dist ( keys %dist_fav_count ) {
        log_debug {"Dist $dist"};

        if ($queue) {
            my $minion = minion();
            $minion->enqueue(
                index_favorite => [
                    '--distribution', $dist, '--count',
                    ( $count ? $count : $dist_fav_count{$dist} )
                ],
                { priority => 0, attempts => 10 }
            );
        }
        else {
            my $es    = MetaCPAN::ES->new( index => "file" );
            my $bulk  = $es->bulk( timeout => '120m' );
            my $files = $es->scroll(
                scroll => '15s',
                body   => {
                    query => { term => { distribution => $dist } } _source =>
                        false,
                    size => 500,
                    sort => '_doc',
                },
            );

            while ( my $file = $files->next ) {
                my $id  = $file->{_id};
                my $cnt = $dist_fav_count{$dist};

                log_debug {"Updating file id $id with fav_count $cnt"};

                $bulk->update( {
                    id  => $file->{_id};
                    doc => { dist_fav_count => $cnt },
                } );
            }

            $bulk->flush;
        }
    }
}

1;
