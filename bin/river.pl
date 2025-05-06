use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS qw< decode_json >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< read_url >;

# setup
my $river_url //= 'https://neilb.org/river-of-cpan.json.gz';
my $river_data = decode_json( read_url($river_url) );

my $es   = MetaCPAN::ES->new( index => "distribution" );
my $bulk = $es->bulk();

log_info {'Updating the distribution index'};

for my $data ( @{$river_data} ) {
    my $dist = delete $data->{dist};

    $bulk->update( {
        id  => $dist,
        doc => {
            name  => $dist,
            river => $data,
        },
        doc_as_upsert => 1,
    } );
}

$bulk->flush;

1;

__END__

=pod

=head1 SYNOPSIS

 # bin/river

=head1 DESCRIPTION

Retrieves the CPAN river data from its source and
updates our ES information.

This can then be accessed here:

http://fastapi.metacpan.org/v1/distribution/Moose
http://fastapi.metacpan.org/v1/distribution/HTTP-BrowserDetect

=cut
