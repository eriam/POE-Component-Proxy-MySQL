#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'POE::Component::Proxy::MySQL' ) || print "Bail out!
";
}

diag( "Testing POE::Component::Proxy::MySQL $POE::Component::Proxy::MySQL::VERSION, Perl $], $^X" );
