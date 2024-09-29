use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< cpan_file_map >;

# args
my ( $distribution, $files_only, $undo );
GetOptions(
    "distribution=s" => \$distribution,
    "files_only"     => \$files_only,
    "undo"           => \$undo,
);

# setup
my $cpan_file_map = cpan_file_map();
my $es_release    = MetaCPAN::ES->new( type => "release" );
my $es_file       = MetaCPAN::ES->new( type => "file" );

my %bulk;
my %release_status;

build_release_status_map();
update_releases() unless $files_only;
update_files();

$_->flush for values %bulk;

log_info {"done"};

###

sub build_release_status_map () {
    log_info {"find_releases"};

    my $scroll = $es_release->scroll(
        fields => [qw< author archive name >],
        body   => get_release_query(),
    );

    while ( my $release = $scroll->next ) {
        my $author  = $release->{fields}{author}[0];
        my $archive = $release->{fields}{archive}[0];
        my $name    = $release->{fields}{name}[0];
        next unless $name;    # bypass some broken releases

        $release_status{$author}{$name} = [
            (
                $undo
                    or exists $cpan_file_map->{$author}{$archive}
            )
            ? 'cpan'
            : 'backpan',
            $release->{_id}
        ];
    }
}

sub get_release_query () {
    unless ($undo) {
        return +{
            query => {
                not => { term => { status => 'backpan' } }
            }
        };
    }

    return +{
        query => {
            bool => {
                must => [
                    { term => { status => 'backpan' } },
                    (
                        $distribution
                        ? { term => { distribution => $distribution } }
                        : ()
                    )
                ]
            }
        }
    };
}

sub update_releases () {
    log_info {"update_releases"};

    $bulk{release} ||= $es_release->bulk( timeout => '5m' );

    for my $author ( keys %release_status ) {

        # value = [ status, _id ]
        for ( values %{ $release_status{$author} } ) {
            $bulk{release}->update( {
                id  => $_->[1],
                doc => {
                    status => $_->[0],
                }
            } );
        }
    }
}

sub update_files () {
    for my $author ( keys %release_status ) {
        my @releases = keys %{ $release_status{$author} };
        while ( my @chunk = splice @releases, 0, 1000 ) {
            update_files_author( $author, \@chunk );
        }
    }
}

sub update_files_author ( $author, $author_releases ) {
    log_info { "update_files: " . $author };

    my $scroll_file = $es_file->scroll(
        scroll => '5m',
        fields => [qw< release >],
        body   => {
            query => {
                bool => {
                    must => [
                        { term  => { author  => $author } },
                        { terms => { release => $author_releases } }
                    ]
                }
            }
        },
    );

    $bulk{file} ||= $es_file->bulk( timeout => '5m' );

    while ( my $file = $scroll_file->next ) {
        my $release = $file->{fields}{release}[0];
        $bulk{file}->update( {
            id  => $file->{_id},
            doc => {
                status => $release_status{$author}{$release}[0]
            }
        } );
    }
}

1;

=pod

=head1 SYNOPSIS

 $ bin/backpan

 $ bin/backpan --distribution DIST

 $ bin/backpan --files_only

 $ bin/backpan --undo ...

=head1 DESCRIPTION

Sets "backpan" status on all BackPAN releases.

--undo will set distributions' status back as 'cpan'
--file_only will only fix the 'file' type

=cut
