use strict;
use warnings;
use v5.36;

use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    are_you_sure
    author_dir
>;

# args
my ( $author, $force, $release );
GetOptions(
    "author=s"  => \$author,
    "force"     => \$force,
    "release=s" => \$release,
);

# setup
purge_author() if $author;

log_info {'Done'};

sub purge_author () {

    # confirm
    $release
        ? are_you_sure(
        sprintf(
            "%s's %s release is about to be purged!", $author, $release
        ),
        $force
        )
        : are_you_sure(
        sprintf( "All of %s's releases are about to be purged!", $author ),
        $force );

    my $query = {
        bool => {
            must => [
                { term => { author => $author } },
                (
                    $release
                    ? { term => { release => $release } }
                    : ()
                )
            ]
        }
    };

    purge_ids( index => 'favorite', query => $query );
    purge_ids( index => 'file',     query => $query );
    purge_ids( index => 'release',  query => $query );
    if ( !$release ) {
        purge_ids( index => 'author',      id => $author );
        purge_ids( index => 'contributor', id => $author );
    }
}

sub purge_ids (%args) {
    my $es   = MetaCPAN::ES->new( index => $args{index} );
    my $bulk = $es->bulk;

    my $id = $args{id};
    my $ids
        = $id
        ? [$id]
        : $es->get_ids( query => $args{query} );

    $bulk->delete_ids(@$ids);

    $bulk->flush;
}

1;

__END__
