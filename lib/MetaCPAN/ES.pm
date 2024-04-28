package MetaCPAN::ES;

use strict;
use warnings;
use v5.36;

use Search::Elasticsearch;

sub new ( $class, %args ) {
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

sub index_refresh ($self) {
    $self->{es}->indices->refresh( index => $self->{index} );
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
    );
}

sub count {
    my ( $self, %args ) = @_;
    return $self->{es}->count(
        index => $self->{index},
        type  => $self->{type},
        body  => $args{body},
    );
}

1;
