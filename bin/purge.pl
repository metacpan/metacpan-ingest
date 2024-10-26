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
my ( $author, $release, $force );
GetOptions(
    "author=s"  => \$author,
    "release=s" => \$release,
    "force"     => \$force,
);

# setup
my $type2index = {
    release      => 'cpan',
    file         => 'cpan',
    author       => 'cpan',
    favorite     => 'cpan',
    permission   => 'cpan',
    contributor  => 'contributor',
};


purge_author() if $author;

log_info {'Done'};

sub purge_author () {
    # confirm
    $release
        ? are_you_sure( sprintf("%s's %s release is about to be purged!", $author, $release), $force )
        : are_you_sure( sprintf("All of %s's releases are about to be purged!", $author), $force );

    my $query = {
        bool => {
            must => [
                { term => { author => $author } },
                ( $release
                  ? { term => { release => $release } }
                  : ()
                )
            ]
        }
    };

    purge_ids( type => 'favorite', query => $query);
    purge_ids( type => 'file',     query => $query);
    purge_ids( type => 'release',  query => $query);
    if ( !$release ) {
        purge_ids( type => 'author', id => $author );
        purge_ids( type => 'contributor', id => $author );
    }
}

sub purge_ids ( %args ) {
    my $type = $args{type};
    my $es = MetaCPAN::ES->new(
        index => $type2index->{$type},
        type => $type
    );

    my $bulk = $es->bulk;

    my $id = $args{id};
    my $ids = $id
        ? [ $id ]
        : $es->get_ids( query => $args{query} );

    $bulk->delete_ids(@$ids);

    $bulk->flush;
}

1;

__END__
