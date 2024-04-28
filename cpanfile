use strict;
use warnings;

requires 'Archive::Any';
requires 'CPAN::DistnameInfo', '0.12';
requires 'Cpanel::JSON::XS';
requires 'Data::Printer';
requires 'DateTime';
requires 'Data::Printer';
requires 'Digest::SHA';
requires 'Email::Valid';
requires 'Encode';
requires 'File::Find::Rule';
requires 'Getopt::Long';
requires 'LWP::UserAgent';
requires 'LWP::Protocol::https';
requires 'PAUSE::Permissions';
requires 'Path::Iterator::Rule';
requires 'Path::Tiny', '0.076';
requires 'PerlIO::gzip';
requires 'Ref::Util';
requires 'PerlIO::gzip';
requires 'Search::Elasticsearch';
requires 'Search::Elasticsearch::Client::2_0';
requires 'Sub::Exporter';
requires 'URI';
requires 'XML::Simple';

requires 'MetaCPAN::Common',
    git => 'https://github.com/metacpan/MetaCPAN-Common',
    ref => '48274b9cb890d7f76a8ba6e2fce78348ca1165ca';

on test => sub {
    requires 'Code::TidyAll',                     '>= 0.74';
    requires 'Code::TidyAll::Plugin::Test::Vars', '0.04';
    requires 'Perl::Critic',                      '1.136';
    requires 'Perl::Tidy' => '20230909';
    requires 'Test::Code::TidyAll';
    requires 'Test::More', '0.96';
    requires 'Test::Perl::Critic';
};
