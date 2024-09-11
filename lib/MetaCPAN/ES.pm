package MetaCPAN::ES;

use strict;
use warnings;
use v5.36;

use Search::Elasticsearch;

use MetaCPAN::Ingest qw< config >;

sub new ( $class, %args ) {
    my $node  = $args{node};
    my $index = $args{index} // "cpan";

    my $config = config;
    $node ||= $config->{config}{es_node};
    $node or die "Cannot create an ES instance without a node\n";

    return bless {
        es => Search::Elasticsearch->new(
            client => '2_0::Direct',
            nodes  => [$node],
        ),
        index => $index,
        type  => $args{type},
    }, $class;
}

sub index ( $self, %args ) {
    $self->{es}->index(
        index => $self->{index},
        type  => $self->{type},
        %args,
    );
}

sub index_refresh ($self) {
    $self->{es}->indices->refresh( index => $self->{index} );
}

sub exists ( $self, %args ) {
    my $id    = $args{id} or die "Missing id\n";
    my $index = $args{index} // $self->{index};
    my $type  = $args{type}  // $self->{type};

    return $self->{es}->exists(
        index => $index,
        type  => $type,
        id    => $id,
    );
}

sub get ( $self, %args ) {
    my $id    = $args{id} or die "Missing id\n";
    my $index = $args{index} // $self->{index};
    my $type  = $args{type}  // $self->{type};

    return $self->{es}->get(
        index => $index,
        type  => $type,
        id    => $id,
    );
}

sub search ( $self, %args ) {
    my $body = $args{body} or die "Missing body\n";

    my $index  = $args{index} // $self->{index};
    my $type   = $args{type}  // $self->{type};
    my @fields = ( $args{fields} ? ( fields => $args{fields} ) : () );
    my @size   = ( $args{size}   ? ( size   => $args{size} )   : () );

    return $self->{es}->search(
        index => $index,
        type  => $type,
        body  => $body,
        @fields,
        @size,
    );
}

sub bulk ( $self, %args ) {
    return $self->{es}->bulk_helper(
        index     => $self->{index},
        type      => $self->{type},
        max_count => ( $args{max_count} // 250 ),
        timeout   => ( $args{timeout}   // '25m' ),
    );
}

sub scroll ( $self, %args ) {
    return $self->{es}->scroll_helper(
        index       => $self->{index},
        type        => $self->{type},
        size        => ( $args{size} // 500 ),
        body        => ( $args{body} // { query => { match_all => {} } } ),
        search_type => 'scan',
        scroll      => ( $args{scroll} // '30m' ),
        ( $args{fields} ? ( fields => $args{fields} ) : () ),
    );
}

sub count ( $self, %args ) {
    return $self->{es}->count(
        index => $self->{index},
        type  => $self->{type},
        body  => $args{body},
    );
}

1;
