use strict;
use warnings;

requires 'Archive::Any';
requires 'CPAN::DistnameInfo', '0.12';
requires 'Cpanel::JSON::XS';
requires 'DateTime';
requires 'DateTime::Format::ISO8601';
requires 'Data::Printer';
requires 'DBI';
requires 'Digest::SHA';
requires 'Email::Valid';
requires 'Email::Sender::Simple';
requires 'Encode';
requires 'File::Find::Rule';
requires 'Getopt::Long';
requires 'IO::Prompt::Tiny';
requires 'List::AllUtils';
requires 'LWP::UserAgent';
requires 'LWP::Protocol::https';
requires 'Net::GitHub::V4';
requires 'PAUSE::Permissions';
requires 'Parse::CPAN::Packages::Fast';
requires 'Path::Iterator::Rule';
requires 'Path::Tiny', '0.076';
requires 'PerlIO::gzip';
requires 'Plack::MIME';
requires 'Ref::Util';
requires 'Regexp::Common';
requires 'Regexp::Common::time';
requires 'Regexp::IPv6';
requires 'Search::Elasticsearch';
requires 'Search::Elasticsearch::Client::2_0';
requires 'Sub::Exporter';
requires 'Text::CSV_XS';
requires 'Term::ANSIColor';
requires 'URI';
requires 'XML::Simple';
requires 'YAML::XS';

requires 'MetaCPAN::Common',
    git => 'https://github.com/metacpan/MetaCPAN-Common',
    ref => '0e5c2852178a47e98db291f613da2b96aca85f64';

on test => sub {
    requires 'App::perlimports';
    requires 'Perl::Critic', '0.140';
    requires 'Perl::Tidy' => '== 20260204';
    requires 'Code::TidyAll::Plugin::Test::Vars', '0.04';
};
