package MyMySQL::Hello;

use POE;
use MooseX::MethodAttributes::Role;

sub fortune : Regexp('qr{Hello}io') {
   my ($self) = $_[OBJECT];
      
   $self->client_send_results(['Hello'],[['World']]);

}



1;
