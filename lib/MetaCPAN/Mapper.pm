package MetaCPAN::Mapper;

use strict;
use warnings;
use v5.36;

use Path::Tiny qw< path >;
use MetaCPAN::Logger qw< :log :dlog >;
use Search::Elasticsearch;
use MetaCPAN::Ingest qw< config home >;

sub new ( $class, %args ) {
    my $node = $args{node};

    my $config = config;
    $node ||= $config->{es_node};
    $node or die "Cannot create an ES instance without a node\n";

    return bless {
        es => Search::Elasticsearch->new(
            client => '2_0::Direct',
            nodes  => [$node],
        ),
    }, $class;
}

sub index_exists ($self, $index) {
    $self->{es}->indices->exists( index => $index );
}

sub index_create ($self, $index) {
    $self->{es}->indices->create( index => $index );
}

sub index_delete ($self, $index) {
    $self->{es}->indices->delete( index => $index );
}

sub index_put_mapping ($self, $index, $type, $mapping) {
    $self->{es}->indices->put_mapping(
        index => $index,
        type  => $type,
        body  => $mapping,
    );
}

sub get_mapping ($self, $index) {
#    my $home = home();
#    my $file = $dir->child('');
}

1;
