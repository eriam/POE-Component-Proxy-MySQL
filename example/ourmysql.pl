#!/usr/bin/env perl

use lib qw( 
   ./lib 
);

use Proc::Daemon;
#use File::Slurp;

#Proc::Daemon::Init;

#
#my $continue = 1;
#$SIG{TERM} = sub { $continue = 0 };

use OurMySQL;

my $server = OurMySQL->new_with_options();

$server->run;


