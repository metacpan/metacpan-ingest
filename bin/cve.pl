use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS qw< decode_json >;
use Getopt::Long;
use Path::Tiny qw< path >;
use Ref::Util qw< is_arrayref >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    config
    cpan_dir
    handle_error
    numify_version
    ua
>;

my %range_ops = qw( < lt <= lte > gt >= gte );

my %valid_keys = map { $_ => 1 } qw<
    affected_versions
    cpansa_id
    cves
    description
    distribution
    references
    releases
    reported
    severity
    versions
>;

# args
my ( $cve_url, $cve_dev_url, $json_file, $test );
GetOptions(
    "cve_url=s"     => \$cve_url,
    "cve_dev_url=s" => \$cve_dev_url,
    "json_file=s"   => \$json_file,
    "test"          => \$test,
);
$cve_url     //= 'https://hackeriet.github.io/cpansa-feed/cpansa.json';
$cve_dev_url //= 'https://hackeriet.github.io/cpansa-feed/cpansa_dev.json';

# setup
my $cpan = cpan_dir();
my $es   = MetaCPAN::ES->new( index => "cve", type => "cve" );
my $bulk = $es->bulk();

my $data = retrieve_cve_data();

log_info {'Updating the cve index'};

for my $dist ( sort keys %{$data} ) {
    for my $cpansa ( @{ $data->{$dist} } ) {
        if ( !$cpansa->{cpansa_id} ) {
            log_warn { sprintf( "Dist '%s' missing cpansa_id", $dist ) };
            next;
        }

        my @matches;

        if ( !is_arrayref( $cpansa->{affected_versions} ) ) {
            log_debug {
                sprintf( "Dist '%s' has non-array affected_versions %s",
                    $dist, $cpansa->{affected_versions} )
            };

            # Temp - remove after fixed upstream
            # (affected_versions will always be an array)
            $cpansa->{affected_versions}
                = [ $cpansa->{affected_versions} ];

            # next;
        }

        my @filters;
        my @afv_filters;

        for my $afv ( @{ $cpansa->{affected_versions} } ) {

            # Temp - remove after fixed upstream
            # (affected_versions will always be an array)
            next unless $afv;

            my @rules = map {s/\(.*?\)//gr} split /,/, $afv;

            my @rule_filters;

            for my $rule (@rules) {
                my ( $op, $num ) = $rule =~ /^([=<>]*)(.*)$/;
                $num = numify_version($num);

                if ( !$op ) {
                    log_debug {
                        sprintf(
                            "Dist '%s' - affected_versions has no operator",
                            $dist )
                    };

                    # Temp - remove after fixed upstream
                    # (affected_versions will always have an operator)
                    $op ||= '=';
                }

                if ( exists $range_ops{$op} ) {
                    push @rule_filters,
                        +{
                        range => {
                            version_numified => { $range_ops{$op} => $num }
                        }
                        };
                }
                else {
                    push @rule_filters,
                        +{ term => { version_numified => $num } };
                }
            }

            # multiple rules (csv) in affected_version line -> AND
            if ( @rule_filters == 1 ) {
                push @afv_filters, @rule_filters;
            }
            elsif ( @rule_filters > 1 ) {
                push @afv_filters, { bool => { must => \@rule_filters } };
            }
        }

        # multiple elements in affected_version -> OR
        if ( @afv_filters == 1 ) {
            push @filters, @afv_filters;
        }
        elsif ( @afv_filters > 1 ) {
            push @filters, { bool => { should => \@afv_filters } };
        }

        if (@filters) {
            my $query = {
                query => {
                    bool => {
                        must => [
                            { term => { distribution => $dist } }, @filters,
                        ]
                    }
                },
            };

            my $releases = $es->search(
                index  => 'cpan',
                type   => 'release',
                body   => $query,
                fields => [ "version", "name", "author", ],
                size   => 2000,
            );

            if ( $releases->{hits}{total} ) {
                ## no critic (ControlStructures::ProhibitMutatingListFunctions)
                @matches = map { $_->[0] }
                    sort { $a->[1] <=> $b->[1] }
                    map {
                    my %fields = %{ $_->{fields} };
                    ref $_ and $_ = $_->[0] for values %fields;
                    [ \%fields, numify_version( $fields{version} ) ];
                    } @{ $releases->{hits}{hits} };
            }
            else {
                log_debug {
                    sprintf( "Dist '%s' doesn't have matches.", $dist )
                };
                next;
            }
        }

        my $doc_data = {
            distribution      => $dist,
            cpansa_id         => $cpansa->{cpansa_id},
            affected_versions => $cpansa->{affected_versions},
            cves              => $cpansa->{cves},
            description       => $cpansa->{description},
            references        => $cpansa->{references},
            reported          => $cpansa->{reported},
            severity          => $cpansa->{severity},
            versions          => [ map { $_->{version} } @matches ],
            releases          => [ map {"$_->{author}/$_->{name}"} @matches ],
        };

        for my $k ( keys %{$doc_data} ) {
            delete $doc_data->{$k} unless exists $valid_keys{$k};
        }

        $bulk->update( {
            id            => $cpansa->{cpansa_id},
            doc           => $doc_data,
            doc_as_upsert => 1,
        } );
    }
}

$bulk->flush;

sub retrieve_cve_data {
    return decode_json( path($json_file)->slurp ) if $json_file;

    my $url = $test ? $cve_dev_url : $cve_url;

    log_info { 'Fetching data from ', $url };
    my $ua   = ua();
    my $resp = $ua->get($url);

    handle_error( $resp->status_line, 1 ) unless $resp->is_success;

    # clean up headers if .json.gz is served as gzip type
    # rather than json encoded with gzip
    if ( $resp->header('Content-Type') eq 'application/x-gzip' ) {
        $resp->header( 'Content-Type'     => 'application/json' );
        $resp->header( 'Content-Encoding' => 'gzip' );
    }

    return decode_json( $resp->decoded_content );
}

1;

__END__

=pod

=head1 SYNOPSIS

 # bin/metacpan cve [--test] [json_file]

=head1 DESCRIPTION

Retrieves the CPAN CVE data from its source and
updates our ES information.

=cut
