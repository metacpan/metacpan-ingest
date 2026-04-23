package MetaCPAN::ES;

use strict;
use warnings;
use v5.36;

use MetaCPAN::Logger qw< :log :dlog >;
use Search::Elasticsearch;
use Ref::Util qw< is_hashref >;

use MetaCPAN::Ingest qw< handle_error es_config >;

sub new ( $class, %args ) {
    my $node  = $args{node};
    my $index = $args{index} // 'cpan';

    my $es_config = es_config($node);

    return bless {
        es => Search::Elasticsearch->new( %$es_config ),
        index => $index,
    }, $class;
}

sub test ($self) {
    return !!( ref($self) eq __PACKAGE__
        and ref( $self->{es} )
        and ref( $self->{es} ) =~ /^Search::Elasticsearch/ );
}

sub index ( $self, %args ) {
    $self->{es}->index(
        index => $self->{index},
        %args,
    );
}

sub index_refresh ($self) {
    $self->{es}->indices->refresh( index => $self->{index} );
}

sub exists ( $self, %args ) {
    my $id    = $args{id} or die "Missing id\n";
    my $index = $args{index} // $self->{index};

    return $self->{es}->exists(
        index => $index,
        id    => $id,
    );
}

sub get ( $self, %args ) {
    my $id    = $args{id} or die "Missing id\n";
    my $index = $args{index} // $self->{index};

    return $self->{es}->get(
        index => $index,
        id    => $id,
    );
}

sub search ( $self, %args ) {
    my $body = $args{body} or die "Missing body\n";

    my $index = $args{index} // $self->{index};
    my @size  = ( $args{size} ? ( size => $args{size} ) : () );

    return $self->{es}->search(
        index => $index,
        body  => $body,
        @size,
    );
}

sub update ( $self, %args ) {
    my $index = $args{index} // $self->{index};
    my $id    = $args{id};
    my $doc   = $args{doc};

    if (!$id) {
        log_warn { "ES update called with no 'id'" };
        return;
    }

    if (!is_hashref($doc) or %$doc == 0) {
        log_warn { "ES update called with no or empty 'doc'" };
        return;
    }

    return $self->{es}->update(
        index   => $index,
        id      => $id,
        body    => { %$doc },
        refresh => 1,
    );
}

sub bulk ( $self, %args ) {
    return $self->{es}->bulk_helper(
        index     => $self->{index},
        max_count => ( $args{max_count} // 250 ),
        timeout   => ( $args{timeout}   // '25m' ),
        ( $args{on_success} ? ( on_success => $args{on_success} ) : () ),
        ( $args{on_error}   ? ( on_error   => $args{on_error} )   : () ),
    );
}

sub scroll ( $self, %args ) {
    my $body = $args{body} // { query => { match_all => {} } };
    $body->{sort} = '_doc'; # optimize search in newer ES versions

    return $self->{es}->scroll_helper(
        index       => $self->{index},
        body        => $body,
        scroll      => ( $args{scroll} // '30m' ),
    );
}

sub count ( $self, %args ) {
    return $self->{es}->count(
        index => $self->{index},
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
    return $self->{es}->get_source(
        index => $self->{index},
        id    => $id,
    );
}

sub clear_index ($self) {
    $self->{es}->delete_by_query(
        index => 'my_index',
        body  => {
            query => {
                match_all => {}
            }
        },
        refresh => 1,   # optional
    );
}

sub await ($self) {
    my $timeout = 15;
    my $iready  = 0;
    my $cluster_info = {};
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
