use strict;
use warnings;
use v5.36;

use DateTime ();
use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;

# args
my $all;
my $days = 1;
GetOptions(
    "all"    => \$all,
    "days=i" => \$days
);

if ($all) {
    update_all();
}
else {
    update_days();
}

log_info {"done."};

###

sub update_all () {
    my $dt       = DateTime->new( year => 1994, month => 1 );
    my $end_time = DateTime->now->add( months => 1 );

    while ( $dt < $end_time ) {
        my $gte = $dt->strftime("%Y-%m-%d");
        if ( my $d = $days ) {
            $dt->add( days => $d );
            log_info {"updating suggest data for $d days from: $gte"};
        }
        else {
            $dt->add( months => 1 );
            log_info {"updating suggest data for month: $gte"};
        }

        my $lt    = $dt->strftime("%Y-%m-%d");
        my $range = +{ range => { date => { gte => $gte, lt => $lt } } };

        _update_slice($range);
    }
}

sub update_days () {
    my $gte
        = DateTime->now()->subtract( days => $days )->strftime("%Y-%m-%d");
    my $range = +{ range => { date => { gte => $gte } } };

    log_info {"updating suggest data since: $gte "};

    _update_slice($range);
}

sub _update_slice ($range) {
    my $es = MetaCPAN::ES->new( type => "file" );

    my $files = $es->scroll(
        scroll => '5m',
        fields => [qw< id documentation >],
        body   => {
            query => {
                bool => {
                    must => [
                        { exists => { field => "documentation" } }, $range
                    ],
                }
            }
        },
    );

    my $bulk = $es->bulk( timeout => '5m' );

    while ( my $file = $files->next ) {
        my $documentation = $file->{fields}{documentation}[0];
        my $weight        = 1000 - length($documentation);
        $weight = 0 if $weight < 0;

        $bulk->update( {
            id  => $file->{fields}{id}[0],
            doc => {
                suggest => {
                    input   => [$documentation],
                    payload => { doc_name => $documentation },
                    weight  => $weight,
                }
            },
        } );
    }

    $bulk->flush;
}

__END__

=head1 SYNOPSIS

 # bin/suggest [--all] [--days N]

=head1 DESCRIPTION

After importing releases from CPAN, this script will set the suggest
field for autocompletion searches.
