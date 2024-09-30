use strict;
use warnings;
use v5.36;

use MetaCPAN::Logger qw< :log :dlog >;

use Ref::Util qw< is_hashref is_ref >;
use HTTP::Request::Common qw< GET >;
use URI::Escape qw< uri_escape >;
use Text::CSV_XS    ();
use Net::GitHub::V4 ();

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    config
    read_url
    ua
>;

# setup
my $rt_summary_url //= 'https://rt.cpan.org/Public/bugs-per-dist.tsv';
my $gh_issues_url
    //= 'https://api.github.com/repos/%s/%s/issues?per_page=100';

$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;

my $config   = config();
my $gh_token = $config->{github_token};    ### TODO: add to config

# Some issue with rt.cpan.org's cert

my $gh_graphql = Net::GitHub::V4->new(
    ( $gh_token ? ( access_token => $gh_token ) : () ) );

my $es   = MetaCPAN::ES->new( index => "cpan", type => "distribution" );
my $bulk = $es->bulk();

check_all_distributions();
index_rt_bugs();
index_github_bugs();

1;

###

# make sure all distributions have an entry
sub check_all_distributions () {
    my $es_release     = MetaCPAN::ES->new( type => "release" );
    my $scroll_release = $es_release->scroll(
        fields => ['distribution'],
        body   => {
            query => {
                not => { term => { status => 'backpan' } },
            }
        },
    );

    my %dists;

    while ( my $release = $scroll_release->next ) {
        my $d = $release->{'fields'}{'distribution'}[0];
        $d or next;

        log_debug { sprintf( "Adding missing distribution record: %s", $d ) };

        $dists{$d} = { name => $d };
    }

    _bulk_update( \%dists );
}

# rt issues are counted for all dists (the download tsv contains everything).
sub index_rt_bugs () {
    log_debug {'Fetching RT bugs'};

    my $ua   = ua();
    my $resp = $ua->request( GET $rt_summary_url );

    log_error { $resp->status_line } unless $resp->is_success;

    # NOTE: This is sending a byte string.
    my $summary = _parse_tsv( $resp->content );

    log_info {"writing rt data"};

    _bulk_update($summary);
}

sub _parse_tsv ($tsv) {
    $tsv
        =~ s/^#\s*(dist\s.+)/$1/m; # uncomment the field spec for Text::CSV_XS
    $tsv =~ s/^#.*\n//mg;

    open my $fh, '<', \$tsv;

    # NOTE: This is byte-oriented.
    my $tsv_parser = Text::CSV_XS->new( { sep_char => "\t" } );
    $tsv_parser->header($fh);

    my %summary;
    while ( my $row = $tsv_parser->getline_hr($fh) ) {
        $summary{ $row->{dist} }{'bugs'}{'rt'} = {
            source => _rt_dist_url( $row->{dist} ),
            active => $row->{active},
            closed => $row->{inactive},
            map      { $_ => $row->{$_} + 0 }
                grep { not /^(dist|active|inactive)$/ }
                keys %$row,
        };
    }

    return \%summary;
}

sub _rt_dist_url ($d) {
    return sprintf( 'https://rt.cpan.org/Public/Dist/Display.html?Name=%s',
        uri_escape($d) );
}

# gh issues are counted for any dist with a github url in `resources.bugtracker.web`.
sub index_github_bugs () {
    log_debug {'Fetching GitHub issues'};

    my $es_release     = MetaCPAN::ES->new( type => "release" );
    my $scroll_release = $es_release->scroll(
        body => {
            query => {
                and => [
                    { term => { status => 'latest' } },
                    {
                        or => [
                            {
                                prefix => {
                                    "resources.bugtracker.web" =>
                                        'http://github.com/'
                                }
                            },
                            {
                                prefix => {
                                    "resources.bugtracker.web" =>
                                        'https://github.com/'
                                }
                            },
                        ],
                    }
                ],
            }
        },
    );

    log_debug { sprintf( "Found %s repos", $scroll_release->total ) };

    my $json = JSON::MaybeXS->new( allow_nonref => 1 );

    my %summary;

RELEASE: while ( my $r = $scroll_release->next ) {
        my $resources = $r->resources;
        my ( $user, $repo, $source )
            = _gh_user_repo_from_resources($resources);
        next unless $user;

        log_debug {"Retrieving issues from $user/$repo"};

        my $data
            = $gh_graphql->query( _gh_graphql_query( $json, $user, $repo ) );

        if ( my $error = $data->{errors} ) {
            for my $error (@$error) {
                my $log_message = sprintf "[%s] %s", $r->{distribution},
                    $error->{message};
                if ( $error->{type} eq 'NOT_FOUND' ) {
                    delete $summary{ $r->{distribution} }{bugs}{github};
                    log_info {$log_message};
                }
                else {
                    log_error {$log_message};
                }
                next RELEASE;
            }
        }

        my $open = $data->{data}{repository}{openIssues}{totalCount}
            + $data->{data}{repository}{openPullRequests}{totalCount};

        my $closed = $data->{data}{repository}{closedIssues}{totalCount}
            + $data->{data}{repository}{closedPullRequests}{totalCount};

        my $rec = {
            active => $open,
            open   => $open,
            closed => $closed,
            source => $source,

        };

        $summary{ $r->{distribution} }{bugs}{github} = $rec;
    }

    log_info {"writing github data"};

    _bulk_update( \%summary );
}

# Try (recursively) to find a github url in the resources hash.
# FIXME: This should check bugtracker web exclusively, or at least first.
sub _gh_user_repo_from_resources ($resources) {
    my ( $user, $repo, $source );

    for my $k ( keys %{$resources} ) {
        my $v = $resources->{$k};

        if ( !is_ref($v)
            && $v
            =~ /^(https?|git):\/\/github\.com\/([^\/]+)\/([^\/]+?)(\.git)?\/?$/
            )
        {
            return ( $2, $3, $v );
        }

        ( $user, $repo, $source ) = _gh_user_repo_from_resources($v)
            if is_hashref($v);

        return ( $user, $repo, $source ) if $user;
    }

    return ();
}

sub _gh_graphql_query ( $json, $user, $repo ) {
    sprintf <<END_QUERY, map $json->encode($_), $user, $repo;
query {
    repository(owner: %s, name: %s) {
        openIssues: issues(states: OPEN) {
            totalCount
        }
        closedIssues: issues(states: CLOSED) {
            totalCount
        }
        openPullRequests: pullRequests(states: OPEN) {
            totalCount
        }
        closedPullRequests: pullRequests(states: [CLOSED, MERGED]) {
            totalCount
        }
    }
}
END_QUERY
}

sub _bulk_update ($records) {
    for my $d ( keys %$records ) {
        $bulk->update( {
            id            => $d,
            doc           => $records->{$d},
            doc_as_upsert => 1,
        } );
    }
}

__END__

=pod

=head1 SYNOPSIS

 # bin/tickets

=head1 DESCRIPTION

Tracks the number of issues and the source, if the issue
tracker is RT or Github it fetches the info and updates
out ES information.

This can then be accessed here:

http://fastapi.metacpan.org/v1/distribution/Moose
http://fastapi.metacpan.org/v1/distribution/HTTP-BrowserDetect

=cut
