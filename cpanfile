use strict;
use warnings;

requires 'Search::Elasticsearch';
requires 'Search::Elasticsearch::Client::2_0';
requires 'XML::Simple';
requires 'CPAN::DistnameInfo', '0.12';
requires 'Digest::SHA';
requires 'MetaCPAN::Common',
    git => 'https://github.com/metacpan/MetaCPAN-Common',
    ref => '5ba1b573f772d8b0ed2bc732370098986eee6556';
requires 'Path::Tiny', '0.076';

on test => sub {
    requires 'Code::TidyAll',                     '>= 0.74';
    requires 'Code::TidyAll::Plugin::Test::Vars', '0.04';
    requires 'Perl::Critic',                      '1.136';
    requires 'Perl::Tidy' => '20230909';
    requires 'Test::Code::TidyAll';
    requires 'Test::More', '0.96';
    requires 'Test::Perl::Critic';
};
