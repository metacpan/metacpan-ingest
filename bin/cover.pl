use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS qw< decode_json >;
use Getopt::Long;
use Path::Tiny qw< path >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< read_url >;

# args
my ( $json_file, $test );
GetOptions(
    "json=s" => \$json_file,
    "test"   => \$test,
);
my $cover_url     //= 'http://cpancover.com/latest/cpancover.json';
my $cover_dev_url //= 'http://cpancover.com/latest/cpancover_dev.json';

# setup
my %valid_keys
    = map { $_ => 1 } qw< branch condition statement subroutine total >;

my $es_release = MetaCPAN::ES->new( index => "release" );
my $es_cover   = MetaCPAN::ES->new( index => "cover" );
my $bulk_cover = $es_cover->bulk();

my $data = retrieve_cover_data();

log_info {'Updating the cover index'};

for my $dist ( sort keys %{$data} ) {
    for my $version ( keys %{ $data->{$dist} } ) {
        my $release   = $dist . '-' . $version;
        my $rel_check = $es_release->search(
            size => 0,
            body => {
                query => { term => { name => $release } },
            },
        );
        if ( $rel_check->{hits}{total} ) {
            log_info { "Adding release info for '" . $release . "'" };
        }
        else {
            log_warn { "Release '" . $release . "' does not exist." };
            next;
        }

        my %doc_data = %{ $data->{$dist}{$version}{coverage}{total} };

        for my $k ( keys %doc_data ) {
            delete $doc_data{$k} unless exists $valid_keys{$k};
        }

        $bulk_cover->update( {
            id  => $release,
            doc => {
                distribution => $dist,
                version      => $version,
                release      => $release,
                criteria     => \%doc_data,
            },
            doc_as_upsert => 1,
        } );
    }
}

$bulk_cover->flush;

###

sub retrieve_cover_data {
    return decode_json( path($json_file)->slurp ) if $json_file;

    my $url = $test ? $cover_dev_url : $cover_url;

    return decode_json( read_url($url) );
}

1;

__END__

=pod

=head1 SYNOPSIS

 # bin/cover [--test] [json_file]

=head1 DESCRIPTION

Retrieves the CPAN Cover data from its source and
updates our ES information.

=cut
