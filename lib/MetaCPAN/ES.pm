package MetaCPAN::ES;

use strict;
use warnings;

use Search::Elasticsearch;

sub new {
    my ( $class, %args ) = @_;
    my $node  = $args{node}  // "elasticsearch:9200";
    my $index = $args{index} // "cpan";

    return bless {
        es => Search::Elasticsearch->new(
            client => '2_0::Direct',
            nodes  => [$node],
        ),
        index => $index,
        type  => $args{type},
    }, $class;
}

sub index_refresh {
    my ($self) = @_;
    $self->{es}->indices->refresh( index => $self->{index} );
}

sub bulk {
    my ( $self, %args ) = @_;
    return $self->{es}->bulk_helper(
        index     => $self->{index},
        type      => $self->{type},
        max_count => ( $args{max_count} // 250 ),
        timeout   => ( $args{timeout}   // '25m' ),
    );
}

sub scroll {
    my ( $self, %args ) = @_;
    return $self->{es}->scroll_helper(
        index       => $self->{index},
        type        => $self->{type},
        size        => ( $args{size} // 500 ),
        body        => ( $args{body} // { query => { match_all => {} } } ),
        search_type => 'scan',
        scroll      => ( $args{scroll} // '30m' ),
    );
}

1;
