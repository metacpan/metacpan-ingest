use strict;
use warnings;
use v5.36;

use MetaCPAN::Logger qw< :log :dlog >;
use Ref::Util        qw< is_arrayref >;

use Getopt::Long;

use MetaCPAN::ES;
use MetaCPAN::Ingest qw< false >;

# args
my ( $age, $all, $distribution, $release );
GetOptions(
    "age=i"          => \$age,
    "all"            => \$all,
    "distribution=s" => \$distribution,
    "release=s"      => \$release,
);

# setup
my $author_mapping = {};
my $email_mapping  = {};

my $es_author      = MetaCPAN::ES->new( index => 'author' );
my $es_release     = MetaCPAN::ES->new( index => "release" );
my $es_contributor = MetaCPAN::ES->new( index => "contributor" );

run();

$es_contributor->index_refresh;

log_info {"done"};

###

sub author_release () {
    return unless $release;
    my ( $author, $release ) = split m{/}, $release;
    $author && $release
        or die
        "Error: invalid 'release' argument (format: PAUSEID/DISTRIBUTION-VERSION)";
    return +{
        author  => $author,
        release => $release,
    };
}

sub run () {
    my $query
        = $all          ? query_all()
        : $distribution ? query_distribution()
        : $release      ? query_release()
        : $age          ? query_age()
        :                 return;

    update_contributors($query);
}

sub query_all () {
    return { match_all => {} };
}

sub query_age () {
    return { range => { date => { gte => sprintf( 'now-%dd', $age ) } } };
}

sub query_distribution () {
    return { term => { distribution => $distribution } };
}

sub query_release () {
    my $author_release = author_release();
    return {
        bool => {
            must => [
                { term => { author => $author_release->{author} } },
                { term => { name   => $author_release->{release} } },
            ]
        }
    };
}

sub update_contributors ($query) {
    my $scroll_release = $es_release->scroll(
        body => {
            query   => $query,
            sort    => ['_doc'],
            _source => [ qw<
                name
                author
                distribution
                metadata.author
                metadata.x_contributors
            > ],
        },
    );

    my $report = sub {
        my ( $action, $result, $i ) = @_;
        if ( $i == 0 ) {
            log_info {'flushing contributor updates'};
        }
    };

    my $bulk_contributor = $es_contributor->bulk(
        on_success => $report,
        on_error   => $report,
    );

    my $total = $scroll_release->total;
    log_info {"updating contributors for $total releases"};

    my $i = 0;
    while ( my $release = $scroll_release->next ) {
        $i++;
        my $source = $release->{_source};
        my $name   = $source->{name};
        if ( !( $name && $source->{author} && $source->{distribution} ) ) {
            Dlog_warn {"found broken release: $_"} $release;
            next;
        }
        log_debug {"updating contributors for $name ($i/$total)"};
        my $actions = release_contributor_update_actions( $release->{_source},
            $es_contributor );
        for my $action (@$actions) {
            $bulk_contributor->add_action(%$action);
        }
    }

    $bulk_contributor->flush;
}

sub release_contributor_update_actions ( $release, $es_contributor ) {
    my @actions;

    my $res = $es_contributor->search(
        body => {
            query => {
                bool => {
                    must => [
                        { term => { release_name   => $release->{name} } },
                        { term => { release_author => $release->{author} } },
                    ],
                }
            },
            sort    => ['_doc'],
            size    => 500,
            _source => false,
        },
    );
    my @ids = map $_->{_id}, @{ $res->{hits}{hits} };
    push @actions, map +{ delete => { id => $_ } }, @ids;

    my $contribs = get_contributors($release);
    my @docs     = map {
        ;
        my $contrib = $_;
        {
            release_name   => $release->{name},
            release_author => $release->{author},
            distribution   => $release->{distribution},
            map +( defined $contrib->{$_} ? ( $_ => $contrib->{$_} ) : () ),
            qw(pauseid name email)
        };
    } @$contribs;
    push @actions, map +{ create => { _source => $_ } }, @docs;
    return \@actions;
}

sub get_contributors ($release) {
    my $author_name = $release->{author};
    my $contribs    = $release->{metadata}{x_contributors} || [];
    my $authors     = $release->{metadata}{author}         || [];

    for ( \( $contribs, $authors ) ) {

        # If a sole contributor is a string upgrade it to an array...
        $$_ = [$$_]
            if !ref $$_;

        # but if it's any other kind of value don't die trying to parse it.
        $$_ = []
            unless Ref::Util::is_arrayref($$_);
    }
    $authors = [ grep { $_ ne 'unknown' } @$authors ];

    my $author_email = $author_mapping->{$author_name}
        //= eval { $es_author->get_source( $author_name )->{email}; }
        or return [];

    my $author_info = {
        email => [
            lc "$author_name\@cpan.org",
            (
                Ref::Util::is_arrayref($author_email)
                ? @{$author_email}
                : $author_email
            ),
        ],
        name => $author_name,
    };
    my %seen = map { $_ => $author_info }
        ( @{ $author_info->{email} }, $author_info->{name}, );

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
        my @fetch_email = grep !exists $email_mapping->{$_},
            sort keys %want_email;

        if (@fetch_email) {
            my $check_author = $es_author->search(
                body => {
                    query   => { terms => { email => \@fetch_email } },
                    _source => [ 'email', 'pauseid' ],
                    size    => 100,
                },
            );

            for my $author ( @{ $check_author->{hits}{hits} } ) {
                my $pauseid = uc $author->{_source}{pauseid};
                my $emails  = $author->{_source}{email};
                $email_mapping->{$_} //= $pauseid
                    for ref $emails ? @$emails : $emails;
            }

            $email_mapping->{$_} //= undef for @fetch_email;
        }

        for my $email ( keys %want_email ) {
            my $pauseid = $email_mapping->{$email}
                or next;
            for my $contrib ( @{ $want_email{$email} } ) {
                $contrib->{pauseid} = $pauseid;
            }
        }
    }

    return \@contribs;
}

1;

__END__

=head1 SYNOPSIS

 # bin/contributor --all
 # bin/contributor --distribution Moose
 # bin/contributor --release ETHER/Moose-2.1806

=head1 DESCRIPTION

Update the list of contributors (CPAN authors only) of all/matching
releases in the 'contributor' index.

=cut
