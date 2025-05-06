use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;
use Cpanel::JSON::XS ();

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< cpan_dir >;

# args
my ( $distribution, $files_only, $undo );
GetOptions(
    "distribution=s" => \$distribution,
    "files_only"     => \$files_only,
    "undo"           => \$undo,
);

# setup
my $cpan = cpan_dir();
my $es   = MetaCPAN::ES->new( index => "mirror" );

index_mirrors();

$es->index_refresh;

# TODO:
# cdn_purge_now( { keys => ['MIRRORS'], } );

log_info {"done"};

###

sub index_mirrors () {
    log_info { 'Getting mirrors.json file from ' . $cpan };

    my $json = $cpan->child( 'indices', 'mirrors.json' )->slurp;

    # Clear out everything in the index
    # so don't end up with old mirrors
    $es->clear_type;

    my $mirrors = Cpanel::JSON::XS::decode_json($json);
    foreach my $mirror (@$mirrors) {
        $mirror->{location} = {
            lon => delete $mirror->{longitude},
            lat => delete $mirror->{latitude}
        };

        #Dlog_trace {"Indexing $_"} $mirror;
        log_debug { sprintf( "Indexing %s", $mirror->{name} ) };

        my @doc = map { $_ => $mirror->{$_} }
            grep { defined $mirror->{$_} }
            keys %$mirror;

        $es->index( body => {@doc} );
    }
}

1;

__END__

=pod

=head1 SYNOPSIS

 $ bin/mirrors.pl

=head1 SOURCE

L<http://www.cpan.org/indices/mirrors.json>

=cut
