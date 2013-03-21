package OurMySQL::HelloWorld;

use MooseX::MethodAttributes::Role;

use MySQL::Packet qw(:test :encode :decode :COM :CLIENT :SERVER :debug);

use POE;

use Socket;
use POSIX qw(errno_h);

use Data::Dumper;
use Date::Format; 
use File::Slurp;


my $packet_count;

sub BUILD {
   print "OurMySQL::HelloWorld->BUILD \n";
}


sub helloworld : Regexp('qr{(HelloWorld\((.*))}io') { #' "
   my ($self, $heap, $session, $query, $tmp_placeholders) = 
      @_[OBJECT, HEAP, SESSION, ARG0, ARG1];

   $self->client_send_results(['Hello'],[['World'],['World']]);

}



1;
