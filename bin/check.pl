use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< read_02packages_fh >;

# args
my ( $module, $max_errors, $errors_only );

GetOptions(
    "module=s"    => \$module,
    "max_errors"  => \$max_errors,
    "errors_only" => \$errors_only,
);

# setup
my $error_count = 0;
my $packages_fh = read_02packages_fh();
my $es_file     = MetaCPAN::ES->new( type => "file" );
my $es_release  = MetaCPAN::ES->new( type => "release" );

my $modules_start = 0;
while ( my $line = <$packages_fh> ) {
    last if $max_errors && $error_count >= $max_errors;
    chomp($line);

    if ($modules_start) {
        my ( $pkg, $ver, $dist ) = split( /\s+/, $line );
        my @releases;

        # we only care about packages if we aren't searching for a
        # particular module or if it matches
        if ( !$module || $pkg eq $module ) {

            # look up this module in ElasticSearch and see what we have on it
            my $results = $es_file->search(
                size   => 100,    # shouldn't get more than this
                fields => [
                    qw< name release author distribution version authorized indexed maturity date >
                ],
                query  => { match_all => {} },
                filter => {
                    and => [
                        { term => { 'module.name' => $pkg } },
                        { term => { authorized    => 'true' } },
                        { term => { maturity      => 'released' } },
                    ],
                },
            );
            my @files = @{ $results->{hits}{hits} };

            # now find the first latest releases for these files
            foreach my $file (@files) {
                my $release_results = $es_release->search(
                    size   => 1,
                    fields => [qw< name status authorized version id date >],
                    query  => { match_all => {} },
                    filter => {
                        and => [
                            { term => { name => $file->{fields}{release} } },
                            { term => { status => 'latest' } },
                        ],
                    },
                );

                push @releases, $release_results->{hits}{hits}[0]
                    if $release_results->{hits}{hits}[0];
            }

            # if we didn't find the latest release, then look at all of the
            # releases so we can find out what might be wrong
            if ( !@releases ) {
                foreach my $file (@files) {
                    my $release_results = $es_release->search(
                        size   => 1,
                        fields =>
                            [qw< name status authorized version id date >],
                        query  => { match_all => {} },
                        filter => {
                            and => [ {
                                term => { name => $file->{fields}{release} }
                            } ]
                        },
                    );

                    push @releases, @{ $release_results->{hits}{hits} };
                }
            }

            # if we found the releases tell them about it
            if (@releases) {
                if (    @releases == 1
                    and $releases[0]->{fields}{status} eq 'latest' )
                {
                    log_info {
                        "Found latest release $releases[0]->{fields}{name} for $pkg"
                    }
                    unless $errors_only;
                }
                else {
                    log_error {"Could not find latest release for $pkg"};
                    foreach my $rel (@releases) {
                        log_warn {"  Found release $rel->{fields}{name}"};
                        log_warn {"    STATUS    : $rel->{fields}{status}"};
                        log_warn {
                            "    AUTORIZED : $rel->{fields}{authorized}"
                        };
                        log_warn {"    DATE      : $rel->{fields}{date}"};
                    }

                    $error_count++;
                }
            }
            elsif (@files) {
                log_error {
                    "Module $pkg doesn't have any releases in ElasticSearch!"
                };
                foreach my $file (@files) {
                    log_warn {"  Found file $file->{fields}{name}"};
                    log_warn {"    RELEASE    : $file->{fields}{release}"};
                    log_warn {"    AUTHOR     : $file->{fields}{author}"};
                    log_warn {
                        "    AUTHORIZED : $file->{fields}{authorized}"
                    };
                    log_warn {"    DATE       : $file->{fields}{date}"};
                }
                $error_count++;
            }
            else {
                log_error {
                    "Module $pkg [$dist] doesn't not appear in ElasticSearch!"
                };
                $error_count++;
            }
            last if $module;
        }
    }
    elsif ( !length $line ) {
        $modules_start = 1;
    }
}

log_info {"done"};

1;
__END__

=pod

=head1 SYNOPSIS

Performs checks on the MetaCPAN data store to make sure an
author/module/distribution has been indexed correctly and has the
appropriate information.

=head2 check_modules

Checks all of the modules in CPAN against the information in ElasticSearch.
If is C<module> attribute exists, it will just look at packages that match
that module name.

=head1 TODO

=over

=item * Add support for checking authors

=item * Add support for checking releases

=back

=cut
