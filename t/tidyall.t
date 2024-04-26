use strict;
use warnings;

use Test::Code::TidyAll qw( tidyall_ok );
use Test::More
    do { $ENV{COVERAGE} ? ( skip_all => 'skip under Devel::Cover' ) : () };
tidyall_ok( verbose => $ENV{TEST_TIDYALL_VERBOSE} );

done_testing();
