use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use CPAN::DistnameInfo;
use Parse::CPAN::Packages::Fast;
use Ref::Util      qw< is_arrayref is_hashref >;
use Regexp::Common qw< time >;
use Time::Local    qw< timelocal >;

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

log_info {'Dry run: updates will not be written to ES'} if $dry_run;

my $minion;
$minion = minion() if $queue;

my $node = config->{es_test_node};

run();

sub run () {
    log_info {'Reading 02packages.details'};

    my $es = MetaCPAN::ES->new(
        type => "file",
        node => $node
    );

    my $packages = read_02packages();

    # If a distribution name is passed get all the package names
    # from 02packages that match that distribution so we can limit
    # the ES query to just those modules.
    my @filter;
    if ( my $d = $distribution ) {
        for my $p ( $packages->packages ) {
            my $dist = $packages->package($p)->distribution->dist;
            push @filter, $p if $dist && $dist eq $d;
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
        my $q_body = _body_query($filter);

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

        my $i = 0;

        # For each file...
        while ( my $file = $scroll->next ) {
            $i++;
            log_debug { "$i of " . $scroll->total } unless ( $i % 100 );
            my $file_data = $file->{_source};

       # Convert module name into Parse::CPAN::Packages::Fast::Package object.
            my @modules = grep {defined}
                map {
                eval { $packages->package( $_->{name} ) }
                } @{ $file_data->{module} };

            # For each of the packages in this file...
            foreach my $module (@modules) {

           # Get P:C:P:F:Distribution (CPAN::DistnameInfo) object for package.
                my $dist = $module->distribution;

                if ($queue) {
                    my $d = $dist->dist;
                    _queue_latest($d)
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
                        if ( $upgrade
                        && compare_dates( $upgrade->{date},
                            $file_data->{date} ) );
                    $upgrade{ $file_data->{distribution} } = $file_data;
                }
                elsif ( $file_data->{status} eq 'latest' ) {
                    $downgrade{ $file_data->{release} } = $file_data;
                }
            }
        }
    }

    my $bulk = $es->bulk( type => 'file' );

    my %to_purge;

    while ( my ( $dist, $file_data ) = each %upgrade ) {

        # Don't reindex if already marked as latest.
        # This just means that it hasn't changed (query includes 'latest').
        next if ( !$force and $file_data->{status} eq 'latest' );

        $to_purge{ $file_data->{download_url} } = 1;

        _reindex( $bulk, $file_data, 'latest' );
    }

    while ( my ( $release, $file_data ) = each %downgrade ) {

        # Don't downgrade if this release version is also marked as latest.
        # This could happen if a module is moved to a new dist
        # but the old dist remains (with other packages).
        # This could also include bug fixes in our indexer, PAUSE, etc.
        next
            if ( !$force
            && $upgrade{ $file_data->{distribution} }
            && $upgrade{ $file_data->{distribution} }{release} eq
            $file_data->{release} );

        $to_purge{ $file_data->{download_url} } = 1;

        _reindex( $bulk, $file_data, 'cpan' );
    }

    $bulk->flush;
    $es->index_refresh;

# Call Fastly to purge
# purge_cpan_distnameinfos( [ map CPAN::DistnameInfo->new($_), keys %to_purge ] );
}

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

sub _body_query ($filter) {
    return +{
        query => {
            bool => {
                must => [
                    {
                        nested => {
                            path  => 'module',
                            query => { bool => { must => $filter } }
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
    },
}

sub _queue_latest ( $dist = $distribution ) {
    log_info { "queueing " . $dist };
    $minion->enqueue(
        index_latest =>
            [ ( $force ? '--force' : () ), '--distribution', $dist ],
        { attempts => 3 }
    );
}

sub _get_release ( $es, $author, $name ) {
    my $release = $es->search(
        body => {
            query => {
                bool => {
                    must => [
                        { term => { author => $author } },
                        { term => { name   => $name } },
                    ]
                }
            }
        },
        fields => [qw< id name >],
    );

    return {}
        unless is_arrayref( $release->{hits}{hits} )
        && is_hashref( $release->{hits}{hits}[0] );

    my $fields = $release->{hits}{hits}[0]{fields};

    return +{
        id   => $fields->{id},
        name => $fields->{name}[0],
    };
}

sub _set_release_status ( $es, $release_id, $status ) {
    my $bulk = $es->bulk();
    $bulk->update( { id => $release_id, doc => { status => $status } } );
    $bulk->flush;
}

# Update the status for the release and all the files.
sub _reindex ( $bulk, $source, $status ) {

    # Update the status on the release.
    my $es_release = MetaCPAN::ES->new(
        type => "release",
        node => $node
    );

    my $release
        = _get_release( $es_release, $source->{author}, $source->{release} );

    unless ( keys %$release ) {
        log_info {
            sprintf( 'failed to fetch release: %s - %s',
                $source->{author}, $source->{release} )
        };
        return;
    }

    _set_release_status( $es_release, $release->{id}, $status )
        unless $dry_run;

    log_info {
        $status eq 'latest' ? 'Upgrading ' : 'Downgrading ',
        'release ', $release->{name}
    };

    # Get all the files for the release.

    my $es_file = MetaCPAN::ES->new(
        type => "file",
        node => $node
    );

    my $scroll = $es_file->scroll(
        body => {
            query => {
                bool => {
                    must => [
                        {
                            term =>
                            { 'release' => $source->{release} }
                        },
                        { term => { 'author' => $source->{author} } },
                    ],
                },
            },
            fields => [qw< name >],
        },
    );

    while ( my $row = $scroll->next ) {
        log_trace {
            sprintf( '%s file %s',
                ( $status eq 'latest' ? 'Upgrading' : 'Downgrading' ),
                $row->{fields}{name}[0] )
        };

        # Use bulk update to overwrite the status for X files at a time.
        $bulk->update( { id => $row->{_id}, doc => { status => $status } } )
            unless $dry_run;
    }
}

sub compare_dates ( $d1, $d2 ) {
    for ( $d1, $d2 ) {
        if ( $_ =~ /$RE{time}{iso}{-keep}/ ) {
            $_ = timelocal( $7, $6, $5, $4, $3 - 1, $2 );
        }
    }
    return $d1 > $d2;
}

1;

__END__

=head1 SYNOPSIS

 # bin/latest

 # bin/latest --dry_run

=head1 DESCRIPTION

After importing releases from cpan, this script will set the status
to latest on the most recent release, its files and dependencies.
It also makes sure that there is only one latest release per distribution.
