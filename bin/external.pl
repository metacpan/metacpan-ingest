use strict;
use warnings;
use v5.36;

use Email::Sender::Simple ();
use Email::Simple         ();
use Getopt::Long;
use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::ES;
use MetaCPAN::External::Cygwin qw< run_cygwin >;
use MetaCPAN::External::Debian qw< run_debian >;

# args
my ( $email_to, $external_source );
GetOptions(
    "email_to=s"        => \$email_to,
    "external_source=s" => \$external_source,
);

die "wrong external source: $external\n"
    unless $external_source
    and grep { $_ eq $external_source } qw< cygwin debian >;

# setup
my $es = MetaCPAN::ES->new( index => "distribution" );
my $bulk = $es->bulk();
my $scroll = $es->scroll(
    scroll => '10m',
    body   => {
        query => {
            exists => { field => "external_package." . $external_source }
        }
    },
);



my $ret;

$ret = run_cygwin() if $external_source eq 'cygwin';
$ret = run_debian() if $external_source eq 'debian';

my $email_body = $ret->{errors_email_body};
if ( $email_to and $email_body ) {
    my $email = Email::Simple->create(
        header => [
            'Content-Type' => 'text/plain; charset=utf-8',
            To             => $email_to,
            From           => 'noreply@metacpan.org',
            Subject => "Package mapping failures report for $external_source",
            'MIME-Version' => '1.0',
        ],
        body => $email_body,
    );
    Email::Sender::Simple->send($email);

    log_debug { "Sending email to " . $email_to . ":" };
    log_debug {"Email body:"};
    log_debug {$email_body};
}

my @to_remove;

while ( my $s = $scroll->next ) {
    my $name = $s->{_source}{name};
    next unless $name;

    if ( exists $dist->{$name} ) {
        delete $dist->{$name}
            if $dist->{$name} eq
            $s->{_source}{external_package}{$external_source};
    }
    else {
        push @to_remove => $name;
    }
}

for my $d ( keys %{$dist} ) {
    log_debug {"[$external_source] adding $d"};
    $bulk->update( {
        id  => $d,
        doc => +{
            'external_package' => {
                $external_source => $dist->{$d}
            }
        },
        doc_as_upsert => 1,
    } );
}

for my $d (@to_remove) {
    log_debug {"[$external_source] removing $d"};
    $bulk->update( {
        id  => $d,
        doc => +{
            'external_package' => {
                $external_source => undef
            }
        }
    } );
}

$bulk->flush;

1;

=pod

=head1 SYNOPSIS

 # bin/external.pl --external_source SOURCE --email_to EMAIL

=cut
