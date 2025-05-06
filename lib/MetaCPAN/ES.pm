package MetaCPAN::ES;

use strict;
use warnings;
use v5.36;

use MetaCPAN::Logger qw< :log :dlog >;
use Search::Elasticsearch;

use MetaCPAN::Ingest qw< config handle_error is_dev >;

sub new ( $class, %args ) {
    my $node  = $args{node};
    my $index = $args{index} // 'cpan';

    my $mode  = is_dev() ? 'test' : 'local';
    $mode eq 'test' and Log::Log4perl::init('log4perl_test.conf'); # TODO: find a better place

    my $config = config;
    my $config_node =
        $node ? $node :
        $mode eq 'local' ? $config->{es_node} :
        $mode eq 'test'  ? $config->{es_test_node} :
        $mode eq 'prod'  ? $config->{es_production_node} :
        undef;
    $config_node or die "Cannot create an ES instance without a node\n";

    return bless {
        es => Search::Elasticsearch->new(
            client => '2_0::Direct',
            nodes  => [$config_node],
        ),
        index => $index,
        type  => ( $args{type} ? $args{type} : $index ),
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

    my $index = $args{index} // $self->{index};
    my $type  = $args{type}  // $self->{type};
    my @size  = ( $args{size} ? ( size => $args{size} ) : () );

    return $self->{es}->search(
        index => $index,
        type  => $type,
        body  => $body,
        @size,
    );
}

sub bulk ( $self, %args ) {
    return $self->{es}->bulk_helper(
        index     => $self->{index},
        type      => $self->{type},
        max_count => ( $args{max_count} // 250 ),
        timeout   => ( $args{timeout}   // '25m' ),
        ( $args{on_success} ? ( on_success => $args{on_success} ) : () ),
        ( $args{on_error}   ? ( on_error   => $args{on_error} )   : () ),
    );
}

sub scroll ( $self, %args ) {
    return $self->{es}->scroll_helper(
        index       => $self->{index},
        type        => $self->{type},
        body        => ( $args{body} // { query => { match_all => {} } } ),
        search_type => 'scan',
        scroll      => ( $args{scroll} // '30m' ),
    );
}

sub count ( $self, %args ) {
    return $self->{es}->count(
        index => $self->{index},
        type  => $self->{type},
        body  => $args{body},
    );
}

sub get_ids ( $self, %args ) {
    my $query = $args{query};

    my $scroll = $self->scroll(
        query => $query // { match_all => {} },
        sort  => '_doc',
    );

    my @ids;

    while ( my $search = $scroll->next ) {
        push @ids => $search->{_id};
    }

    return \@ids;
}

sub get_source ( $self, $id ) {
    return $self->{es}->get_source($id);
}

sub delete_ids ( $self, $ids ) {
    my $bulk = $self->bulk;

    while ( my @batch = splice( @$ids, 0, 500 ) ) {
        $bulk->delete_ids(@batch);
    }

    $bulk->flush;
}

sub clear_type ($self) {
    my $ids = $self->get_ids();

    $self->delete_ids(@$ids);
}

sub await ($self) {
    my $timeout = 15;
    my $iready  = 0;
    my $cluster_info;
    my $es = $self->{es};

    if ( scalar( keys %$cluster_info ) == 0 ) {
        my $iseconds = 0;

        log_info {"Awaiting Elasticsearch ..."};

        do {
            eval {
                $iready = $es->ping;

                if ($iready) {
                    log_info {
                        sprintf( "Awaiting %d / %d : ready",
                            $iseconds, $timeout )
                    };
                    $cluster_info = \%{ $es->info };
                }
            };

            if ($@) {
                if ( $iseconds < $timeout ) {
                    log_info {
                        sprintf(
                            "Awaiting %d / %d : unavailable - sleeping ...",
                            $iseconds, $timeout )
                    };
                    sleep(1);
                    $iseconds++;
                }
                else {
                    log_info {
                        sprintf( "Awaiting %d / %d : unavailable - timeout!",
                            $iseconds, $timeout )
                    };

                    #Set System Error: 112 - EHOSTDOWN - Host is down
                    handle_error( 112, $@, 1 );
                }
            }
        } while ( !$iready && $iseconds <= $timeout );
    }
    else {
        #ElasticSearch Service is available
        $iready = 1;
    }

    return $iready;
}

1;
