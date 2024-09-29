use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< minion >;

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
    my $body;
    my $age_filter;

    if ($age) {
        $age_filter
            = { range => { date => { gte => sprintf( 'now-%dm', $age ) } } };
    }

    if ($distribution) {
        $body = {
            query => {
                term => { distribution => $distribution }
            }
        };
    }
    elsif ($age) {
        my $es   = MetaCPAN::ES->new( type => "favorite" );
        my $favs = $es->scroll(
            scroll => '5m',
            fields => [qw< distribution >],
            body   => {
                query => $age_filter,
                ( $limit ? ( size => $limit ) : () )
            }
        );

        my %recent_dists;

        while ( my $fav = $favs->next ) {
            my $dist = $fav->{fields}{distribution}[0];
            $recent_dists{$dist}++ if $dist;
        }

        my @keys = keys %recent_dists;
        if (@keys) {
            $body = {
                query => {
                    terms => { distribution => \@keys }
                }
            };
        }
        $es->index_refresh;
    }

    # get total fav counts for distributions

    my %dist_fav_count;

    if ($count) {
        $dist_fav_count{$distribution} = $count;
    }
    else {
        my $es   = MetaCPAN::ES->new( type => "favorite" );
        my $favs = $es->scroll(
            scroll => '30s',
            fields => [qw< distribution >],
            ( $body ? ( body => $body ) : () ),
        );

        while ( my $fav = $favs->next ) {
            my $dist = $fav->{fields}{distribution}[0];
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

        my $es    = MetaCPAN::ES->new( type => "file" );
        my $files = $es->scroll(
            scroll => '15m',
            fields => [qw< id distribution >],
            size   => 500,
            body   => {
                query => {
                    bool => {
                        must_not => [
                            { range => { dist_fav_count => { gte => 1 } } }
                        ],
                        @age_filter,
                    }
                }
            },
        );

        while ( my $file = $files->next ) {
            my $dist = $file->{fields}{distribution}[0];
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
            my $es    = MetaCPAN::ES->new( type => "file" );
            my $bulk  = $es->bulk( timeout => '120m' );
            my $files = $es->scroll(
                scroll => '15s',
                fields => [qw< id >],
                body   => {
                    query => { term => { distribution => $dist } }
                },
            );

            while ( my $file = $files->next ) {
                my $id  = $file->{fields}{id}[0];
                my $cnt = $dist_fav_count{$dist};

                log_debug {"Updating file id $id with fav_count $cnt"};

                $bulk->update( {
                    id  => $file->{fields}{id}[0],
                    doc => { dist_fav_count => $cnt },
                } );
            }

            $bulk->flush;
        }
    }
}

1;
