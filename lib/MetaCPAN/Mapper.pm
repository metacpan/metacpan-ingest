package MetaCPAN::Mapper;

use strict;
use warnings;
use v5.36;

use Search::Elasticsearch ();
use Cpanel::JSON::XS      qw( decode_json );
use MetaCPAN::Ingest      qw( es_config mapping_config );

sub new ( $class, %args ) {
    my $node = $args{node};

    my $es_config = es_config($node);

    return bless {
        es             => Search::Elasticsearch->new(%$es_config),
        mapping_config => mapping_config(),
    }, $class;
}

sub test ($self) {
    return !!( ref($self) eq __PACKAGE__
        and ref( $self->{es} )
        and ref( $self->{es} ) =~ /^Search::Elasticsearch/ );
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
        my $mapping_file
            = $self->{mapping_config}->child( $index . '/mapping.json' );
        my $mapping = decode_json $mapping_file->slurp();
        my $settings_file
            = $self->{mapping_config}->child( $index . '/settings.json' );
        my $settings = decode_json $settings_file->slurp();

        @body = (
            body => {
                settings => $settings,
                mappings => $mapping,
            }
        );
    }

    $self->{es}->indices->create( index => $index, @body );
}

sub index_delete ( $self, $index, $skip_exists = 0 ) {
    return if $skip_exists and !$self->index_exists($index);
    $self->{es}->indices->delete( index => $index );
}

sub index_put_mapping ( $self, $index, $mapping ) {
    $self->{es}->indices->put_mapping(
        index => $index,
        body  => $mapping,
    );
}

sub available_mappings ( $self ) {
    my $mc = $self->{mapping_config};
    return {
        map  { $_->relative($mc) => 1 }
        grep { $_->is_dir() }
        $mc->children()
    };
}

sub index_add_mapping ( $self, $index, $skip_exists = 0 ) {
    return if $skip_exists and !$self->index_exists($index);

    my $map_file = $self->{mapping_config}->child( $index . '/mapping.json' );
    my $mapping  = decode_json $map_file->slurp();

    $self->index_put_mapping( $index, $mapping );
}

1;

__END__

=head1 NAME

MetaCPAN::Mapper - Create and manage Elasticsearch indices for MetaCPAN

=head1 SYNOPSIS

    my $mapper = MetaCPAN::Mapper->new;
    $mapper->index_create( index => 'release', add_mapping => 1 );

=head1 DESCRIPTION

Manages Elasticsearch index lifecycle: creation, deletion, and mapping
updates. Mapping and settings are read from
C<conf/es/$index/mapping.json> and C<conf/es/$index/settings.json>.

=head1 METHODS

=head2 new

    my $mapper = MetaCPAN::Mapper->new( node => $node );

=head2 index_exists

Returns true if the named index exists in Elasticsearch.

=head2 index_create

    $mapper->index_create(
        index        => $name,
        add_mapping  => 1,
        delete_first => 1,
    );

Creates an index. With C<add_mapping>, applies mappings and settings from
C<conf/es/>. With C<delete_first>, removes any existing index first.

=head2 index_delete

    $mapper->index_delete( $index, $skip_if_missing );

Deletes an index. Pass a true second argument to skip if the index does not
exist.

=head2 index_add_mapping

    $mapper->index_add_mapping( $index, $skip_if_missing );

Loads C<conf/es/$index/mapping.json> and applies it to an existing index.

=head2 index_put_mapping

    $mapper->index_put_mapping( $index, \%mapping );

Puts a mapping hashref directly onto an existing index.

=head2 test

Returns true if this is a properly initialised MetaCPAN::Mapper instance.

=cut
