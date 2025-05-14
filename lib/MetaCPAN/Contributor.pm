package MetaCPAN::Contributor;

use strict;
use warnings;
use v5.36;

use Ref::Util qw< is_arrayref >;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< digest >;

use Sub::Exporter -setup => {
    exports => [ qw<
        get_cpan_author_contributors
        update_release_contirbutors
    > ]
};

sub get_cpan_author_contributors ( $author, $release, $distribution ) {
    my $es_contributor = MetaCPAN::ES->new( index => 'contributor' );

    my @ret;

    my $data;
    eval {
        $data = get_contributors( $author, $release );
        1;
    } or return [];

    for my $d ( @{ $data->{contributors} } ) {
        next unless exists $d->{pauseid};

        # skip existing records
        my $id     = digest( $d->{pauseid}, $release );
        my $exists = $es_contributor->exists( id => $id );
        next if $exists;

        $d->{release_author} = $author;
        $d->{release_name}   = $release;
        $d->{distribution}   = $distribution;
        push @ret, $d;
    }

    return \@ret;
}

sub update_release_contirbutors ( $document, $timeout = "5m" ) {
    my $data = get_cpan_author_contributors(
        @{$document}{qw< author name distribution >} );
    return unless $data and is_arrayref($data);

    my $es_contributor   = MetaCPAN::ES->new( index => 'contributor' );
    my $bulk_contributor = $es_contributor->bulk( timeout => $timeout );

    for my $d ( @{$data} ) {
        my $id = digest( $d->{pauseid}, $d->{release_name} );
        $bulk_contributor->update( {
            id  => $id,
            doc => {
                pauseid        => $d->{pauseid},
                release_name   => $d->{release_name},
                release_author => $d->{release_author},
                distribution   => $d->{distribution},
            },
            doc_as_upsert => 1,
        } );
    }

    $bulk_contributor->flush;
}

sub get_contributors ( $author_name, $release_name ) {
    my $es_release = MetaCPAN::ES->new( index => "release" );
    my $es_author  = MetaCPAN::ES->new( index => "author" );

    my $query = +{
        query => {
            bool => {
                must => [
                    { term => { name   => $release_name } },
                    { term => { author => $author_name } },
                ],
            },
        }
    };

    my $res = $es_release->search(
        body => {
            query   => $query,
            size    => 999,
            _source => [qw< metadata.author metadata.x_contributors >],
        }
    );

    my $release  = $res->{hits}{hits}[0]{_source};
    my $contribs = $release->{metadata}{x_contributors} || [];
    my $authors  = $release->{metadata}{author}         || [];

    for ( \( $contribs, $authors ) ) {

        # If a sole contributor is a string upgrade it to an array...
        $$_ = [$$_]
            if !ref $$_;

        # but if it's any other kind of value don't die trying to parse it.
        $$_ = []
            unless Ref::Util::is_arrayref($$_);
    }
    $authors = [ grep { $_ ne 'unknown' } @$authors ];

    # TODO: check if check is still needed -
    # this check is against a failure in tests (because fake author)
    return
        unless $es_author->exists( id => $author_name );

    my $author = $es_author->get( id => $author_name );

    my $author_email        = $author->{_source}{email};
    my $author_gravatar_url = $author->{_source}{gravatar_url};

    my $author_info = {
        email => [
            lc "$author_name\@cpan.org",
            (
                Ref::Util::is_arrayref($author_email) ? @{$author_email}
                : $author_email
            ),
        ],
        name => $author_name,
        (
            $author_gravatar_url ? ( gravatar_url => $author_gravatar_url )
            : ()
        ),
    };

    my %seen = map { $_ => $author_info }
        ( grep {defined} @{ $author_info->{email} }, $author_info->{name}, );

    my @contribs = map {
        my $name = $_;
        my $email;
        if ( $name =~ s/\s*<([^<>]+@[^<>]+)>// ) {
            $email = $1;
        }
        my $info;
        my $dupe;
        if ( $email and $info = $seen{$email} ) {
            $dupe = 1;
        }
        elsif ( $info = $seen{$name} ) {
            $dupe = 1;
        }
        else {
            $info = {
                name  => $name,
                email => [],
            };
        }
        $seen{$name} ||= $info;
        if ($email) {
            push @{ $info->{email} }, $email
                unless grep { $_ eq $email } @{ $info->{email} };
            $seen{$email} ||= $info;
        }
        $dupe ? () : $info;
    } ( @$authors, @$contribs );

    my %want_email;
    for my $contrib (@contribs) {

        # heuristic to autofill pause accounts
        if ( !$contrib->{pauseid} ) {
            my ($pauseid)
                = map { /^(.*)\@cpan\.org$/ ? $1 : () }
                @{ $contrib->{email} };
            $contrib->{pauseid} = uc $pauseid
                if $pauseid;

        }

        push @{ $want_email{$_} }, $contrib for @{ $contrib->{email} };
    }

    if (%want_email) {
        my $check_author = $es_author->search(
            body => {
                query   => { term => { email => [ sort keys %want_email ] } },
                _source => [ 'email', 'pauseid' ],
                size    => 10,
            }
        );

        for my $author ( @{ $check_author->{hits}{hits} } ) {
            my $emails = $author->{_source}{email};
            $emails = [$emails] if !ref $emails;
            my $pauseid = uc $author->{_source}{pauseid};
            for my $email (@$emails) {
                for my $contrib ( @{ $want_email{$email} } ) {
                    $contrib->{pauseid} = $pauseid;
                }
            }
        }
    }

    my $contrib_query = +{
        query => {
            terms => {
                pauseid =>
                    [ map { $_->{pauseid} ? $_->{pauseid} : () } @contribs ]
            }
        }
    };

    my $contrib_authors = $es_author->search(
        body => {
            query   => $contrib_query,
            size    => 999,
            _source => [qw< pauseid gravatar_url >],
        }
    );

    my %id2url = map { $_->{_source}{pauseid} => $_->{_source}{gravatar_url} }
        @{ $contrib_authors->{hits}{hits} };
    for my $contrib (@contribs) {
        next unless $contrib->{pauseid};
        $contrib->{gravatar_url} = $id2url{ $contrib->{pauseid} }
            if exists $id2url{ $contrib->{pauseid} };
    }

    return { contributors => \@contribs };
}

1;

__END__
