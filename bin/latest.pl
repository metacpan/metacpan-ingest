use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use CPAN::DistnameInfo;
use Parse::CPAN::Packages::Fast;
use Regexp::Common qw< time >;
use Time::Local qw< timelocal >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    config
    minion
    read_02packages
>;

# args
my ( $distribution, $dry_run, $force, $queue );
GetOptions(
    "distribution=s" => \$distribution,
    "dry_run"        => \$dry_run,
    "force"          => \$force,
    "queue"          => \$queue,
);

# run

log_info {'Dry run: updates will not be written to ES'} if $dry_run;

my $minion;
$minion = minion() if $queue;

run();

sub run {
    log_info {'Reading 02packages.details'};

    my $packages = read_02packages();

    # If a distribution name is passed get all the package names
    # from 02packages that match that distribution so we can limit
    # the ES query to just those modules.
    my @filter;
    if ( my $d = $distribution ) {
        for my $p ( $packages->packages ) {
            my $dist = $packages->package($p)->distribution->dist;
            push @filter, $p
                if $dist && $dist eq $d;
        }
        log_info {
            "$distribution consists of " . scalar(@filter) . ' modules'
        };
    }

    # if we are just queueing a single distribution
    if ( $queue and $distribution ) {
        _queue_latest($distribution);
        return;
    }

    return if ( !scalar(@filter) && $distribution );

    my %upgrade;
    my %downgrade;
    my %queued_distributions;

    my $total       = scalar(@filter);
    my $found_total = 0;

    my $module_filters = _add_module_filters( \@filter );

    for my $filter (@$module_filters) {

        log_debug {
            sprintf( "Searching for %d of %d modules",
                scalar(@$filter), $total )
        }
        if scalar(@$module_filters) > 1;

        # This filter will be used to produce a (scrolled) list of
        # 'file' type records where the module.name matches the
        # distribution name and which are released &
        # indexed (the 'leading' module)
        my $q_body = _filtered_query($filter);

        my $node   = config->{config}{es_test_node};
        my $es     = MetaCPAN::ES->new( type => "file", node => $node );
        my $scroll = $es->scroll(
            body    => $q_body,
            size    => 100,
            _source => [
                qw< author date distribution download_url module.name release status >
            ],
        );

        $found_total += $scroll->total;

        log_debug { sprintf( "Found %s modules",       $scroll->total ) };
        log_debug { sprintf( "Found %s total modules", $found_total ) }
        if @$filter != $total and $filter == $module_filters->[-1];
        exit;

        my $i = 0;
        while ( my $file = $scroll->next ) {
            use DDP;
            &p( [$file] );
        }
        exit;

    }
}

=head2

        # For each file...
        while ( my $file = $scroll->next ) {
            $i++;
            log_debug { "$i of " . $scroll->total } unless ( $i % 100 );
            my $file_data = $file->{_source};

       # Convert module name into Parse::CPAN::Packages::Fast::Package object.
            my @modules = grep {defined}
                map {
                eval { $p->package( $_->{name} ) }
                } @{ $file_data->{module} };

            # For each of the packages in this file...
            foreach my $module (@modules) {

           # Get P:C:P:F:Distribution (CPAN::DistnameInfo) object for package.
                my $dist = $module->distribution;

                if ( $self->queue ) {
                    my $d = $dist->dist;
                    $self->_queue_latest($d)
                        unless exists $queued_distributions{$d};
                    $queued_distributions{$d} = 1;
                    next;
                }

               # If 02packages has the same author/release for this package...

                # NOTE: CPAN::DistnameInfo doesn't parse some weird uploads
                # (like /\.pm\.gz$/) so distvname might not be present.
                # I assume cpanid always will be.
                if (   defined( $dist->distvname )
                    && $dist->distvname eq $file_data->{release}
                    && $dist->cpanid eq $file_data->{author} )
                {
                    my $upgrade = $upgrade{ $file_data->{distribution} };

                    # If multiple versions of a dist appear in 02packages
                    # only mark the most recent upload as latest.
                    next
                        if (
                        $upgrade
                        && $self->compare_dates(
                            $upgrade->{date}, $file_data->{date}
                        )
                        );
                    $upgrade{ $file_data->{distribution} } = $file_data;
                }
                elsif ( $file_data->{status} eq 'latest' ) {
                    $downgrade{ $file_data->{release} } = $file_data;
                }
            }
        }
    }

=cut

#$bulk->flush;

#$es->index_refresh();

# subs

sub _add_module_filters ($filter) {
    my @module_filters;
    if (@$filter) {
        while (@$filter) {
            my @modules = splice @$filter, 0, 500;
            push @module_filters,
                [
                { term  => { 'module.indexed' => 1 } },
                { terms => { "module.name"    => \@modules } },
                ];
        }
    }
    else {
        push @module_filters,
            [
            { term   => { 'module.indexed' => 1 } },
            { exists => { field            => "module.name" } },
            ];
    }

    return \@module_filters;
}

sub _filtered_query ($filter) {
    return +{
        filtered => {
            filter => {
                bool => {
                    must => [
                        {
                            nested => {
                                path   => 'module',
                                filter => { bool => { must => $filter } }
                            }
                        },
                        { term => { 'maturity' => 'released' } },
                    ],
                    must_not => [
                        { term => { status       => 'backpan' } },
                        { term => { distribution => 'perl' } }
                    ]
                }
            },
            query => { match_all => {} },
        }
    };
}

sub _queue_latest ( $dist = $distribution ) {
    log_info { "queueing " . $dist };
    $minion->enqueue(
        index_latest =>
            [ ( $force ? '--force' : () ), '--distribution', $dist ],
        { attempts => 3 }
    );
}

1;

__END__

sub run {
###





    my $bulk = $self->es->bulk_helper(
        index => $self->index->name,
        type  => 'file'
    );

    my %to_purge;

    while ( my ( $dist, $file_data ) = each %upgrade ) {

        # Don't reindex if already marked as latest.
        # This just means that it hasn't changed (query includes 'latest').
        next if ( !$self->force and $file_data->{status} eq 'latest' );

        $to_purge{ $file_data->{download_url} } = 1;

        $self->reindex( $bulk, $file_data, 'latest' );
    }

    while ( my ( $release, $file_data ) = each %downgrade ) {

        # Don't downgrade if this release version is also marked as latest.
        # This could happen if a module is moved to a new dist
        # but the old dist remains (with other packages).
        # This could also include bug fixes in our indexer, PAUSE, etc.
        next
            if ( !$self->force
            && $upgrade{ $file_data->{distribution} }
            && $upgrade{ $file_data->{distribution} }->{release} eq
            $file_data->{release} );

        $to_purge{ $file_data->{download_url} } = 1;

        $self->reindex( $bulk, $file_data, 'cpan' );
    }
    $bulk->flush;
    $self->index->refresh;

    # Call Fastly to purge
    $self->purge_cpan_distnameinfos( [
        map CPAN::DistnameInfo->new($_), keys %to_purge ] );
}

# Update the status for the release and all the files.
sub reindex {
    my ( $self, $bulk, $source, $status ) = @_;

    # Update the status on the release.
    my $release = $self->index->type('release')->get( {
        author => $source->{author},
        name   => $source->{release},
    } );

    $release->_set_status($status);
    log_info {
        $status eq 'latest' ? 'Upgrading ' : 'Downgrading ',
            'release ', $release->name || q[];
    };
    $release->put unless ( $dry_run );

    # Get all the files for the release.
    my $scroll = $self->index->type("file")->search_type('scan')->filter( {
        bool => {
            must => [
                { term => { 'release' => $source->{release} } },
                { term => { 'author'  => $source->{author} } }
            ]
        }
    } )->size(100)->source( [ 'status', 'file' ] )->raw->scroll;

    while ( my $row = $scroll->next ) {
        my $source = $row->{_source};
        log_trace {
            $status eq 'latest' ? 'Upgrading ' : 'Downgrading ',
                'file ', $source->{name} || q[];
        };

        # Use bulk update to overwrite the status for X files at a time.
        $bulk->update( { id => $row->{_id}, doc => { status => $status } } )
            unless $dry_run;
    }
}

sub compare_dates {
    my ( $self, $d1, $d2 ) = @_;
    for ( $d1, $d2 ) {
        if ( $_ =~ /$RE{time}{iso}{-keep}/ ) {
            $_ = timelocal( $7, $6, $5, $4, $3 - 1, $2 );
        }
    }
    return $d1 > $d2;
}

=head1 SYNOPSIS

 # bin/metacpan latest

 # bin/metacpan latest --dry_run

=head1 DESCRIPTION

After importing releases from cpan, this script will set the status
to latest on the most recent release, its files and dependencies.
It also makes sure that there is only one latest release per distribution.
