package MetaCPAN::Mapper;

use strict;
use warnings;
use v5.36;

use Cpanel::JSON::XS qw< decode_json >;
use Path::Tiny       qw< path >;
use MetaCPAN::Logger qw< :log :dlog >;
use Search::Elasticsearch;
use MetaCPAN::Ingest qw< config home is_dev >;

sub new ( $class, %args ) {
    my $node = $args{node};

    my $mode = is_dev() ? 'test' : 'local';
    $mode eq 'test'
        and Log::Log4perl::init('log4perl_test.conf')
        ;    # TODO: find a better place

    my $config = config;
    my $config_node
        = $node            ? $node
        : $mode eq 'local' ? $config->{es_node}
        : $mode eq 'test'  ? $config->{es_test_node}
        : $mode eq 'prod'  ? $config->{es_production_node}
        :                    undef;
    $config_node or die "Cannot create an ES instance without a node\n";

    return bless {
        es => Search::Elasticsearch->new(
            client => '2_0::Direct',
            nodes  => [$config_node],
        ),
    }, $class;
}

sub index_exists ( $self, $index ) {
    $self->{es}->indices->exists( index => $index );
}

sub index_create ( $self, %args ) {
    my $index = $args{index};
    $index or die "Need an index name to create an index\n";

    my $add_mapping  = $args{add_mapping};
    my $delete_first = $args{delete_first};

    $self->index_delete( $index, 1 ) if ($delete_first);

    my @body;
    if ($add_mapping) {
        my $home = home();
        my $mapping_file
            = $home->child( 'conf/es/' . $index . '/mapping.json' );
        my $mapping = decode_json $mapping_file->slurp();
        my $settings_file
            = $home->child( 'conf/es/' . $index . '/settings.json' );
        my $settings = decode_json $settings_file->slurp();

        @body = (
            body => {
                settings => $settings,
                mappings => { $index => $mapping },
            }
        );
    }

    $self->{es}->indices->create( index => $index, @body );
}

sub index_delete ( $self, $index, $skip_exists ) {
    return if $skip_exists and !$self->index_exists($index);
    $self->{es}->indices->delete( index => $index );
}

sub index_put_mapping ( $self, $index, $mapping ) {
    $self->{es}->indices->put_mapping(
        index => $index,
        type  => $index,
        body  => $mapping,
    );
}

sub index_add_mapping ( $self, $index, $skip_exists ) {
    return if $skip_exists and !$self->index_exists($index);

    my $home     = home();
    my $map_file = $home->child( 'conf/es/' . $index . '/mapping.json' );
    my $mapping  = decode_json $map_file->slurp();

    $self->index_put_mapping( $index, $mapping );
}

1;
