package POE::Component::Proxy::MySQL;
use Moose;
use MooseX::MethodAttributes;

our $VERSION = "0.01";


with 'MooseX::Getopt';

use Socket;
use POSIX qw(errno_h);
use MySQL::Packet qw(:encode :decode :COM :CLIENT :SERVER);
use Data::Dumper;
use Module::Find;
use POE qw( Wheel::ReadWrite Wheel::SocketFactory Filter::Stream );
use POE::Component::Proxy::MySQL::Forwarder;


has 'src_address'    => (is => 'rw', isa => 'Str');
has 'src_port'       => (is => 'rw', isa => 'Int');

has 'dst_address'    => (is => 'rw', isa => 'Str');
has 'dst_port'       => (is => 'rw', isa => 'Int');

has 'processes'      => (is => 'rw', isa => 'Int');

has 'conn_per_child' => (is => 'rw', isa => 'Int');


sub DEBUG {1}

sub BUILD {
	my ($self, $opt) = @_;

   $self->conn_per_child(4096)      unless $self->conn_per_child;

   $self->src_port(23306)           unless $self->src_port;
   $self->src_address('127.0.0.1')  unless $self->src_address;

   $self->dst_port(5029)            unless $self->dst_port;
   $self->dst_address('127.0.0.1')  unless $self->dst_address;
   
   $self->processes(4)              unless $self->processes;
   $self->processes($self->processes - 1);
   $self->processes(1) if $self->processes < 1;
   

   POE::Session->create(
     object_states => [
         $self =>  { 
            _start         => 'server_start',
            _stop          => 'server_stop',
            accept_success => 'server_accept_success',
            accept_failure => 'server_accept_failure',   
            got_sig_int    => 'got_sig_int',
            got_sig_child  => 'got_sig_child',
            _do_fork       => '_do_fork',   
            restart        => 'restart',    
            ticks          => 'ticks',   
         }
     ],
      args => [$self->src_address, $self->src_port, $self->dst_address, $self->dst_port]
   );

   return $self;
}



sub server_start {
   my ($self, $heap, $session, $local_addr, $local_port, $remote_addr, $remote_port) =
    @_[OBJECT, HEAP, SESSION, ARG0, ARG1, ARG2, ARG3];
      
   $heap->{local_addr}     = $local_addr;
   $heap->{local_port}     = $local_port;
   $heap->{remote_addr}    = $remote_addr;
   $heap->{remote_port}    = $remote_port;
   $heap->{is_a_child}     = 0;
   $heap->{processes}      ||= $self->processes;
   $heap->{conn_per_child} ||= $self->conn_per_child;
   $heap->{can_fork}       = 0;
   
   $heap->{server_wheel} = POE::Wheel::SocketFactory->new(
      BindAddress  => $local_addr,
      BindPort     => $local_port,
      Reuse        => 'yes',
      SuccessEvent => 'accept_success',
      FailureEvent => 'accept_failure',
   );

   $heap->{pid} = $$;
   
   if ($heap->{processes} > 0) {
      $_[KERNEL]->yield('_do_fork');
   }
}


sub ticks {
   my ($self, $heap, $kernel) = @_[OBJECT, HEAP, KERNEL];
   
   if ($heap->{is_a_child}) {
      
      if ($heap->{connections_per_child}->{$$} >= $heap->{conn_per_child}
            && $heap->{is_a_child}) {
         $heap->{can_fork} = 1;
         $kernel->yield('_do_fork');
         $heap->{server_wheel}->pause_accept;
      }
      else {
          $kernel->delay_set('ticks', 10);
      }
      
   }

}

sub restart {
   my ($self, $kernel, $heap) = @_[OBJECT, KERNEL, HEAP];
   
   $kernel->delay_set('tick', 60);
}


sub got_sig_int {
   my ($self, $kernel, $heap) = @_[OBJECT, KERNEL, HEAP];
   
  delete $heap->{server};
  $kernel->sig_handled();
}

sub got_sig_child {
  my ($kernel, $heap, $child_pid) = @_[KERNEL, HEAP, ARG1];

  return unless delete $heap->{children}->{$child_pid};

  $kernel->yield("_do_fork") if exists $_[HEAP]->{server_wheel};
}

sub _do_fork {
   my ($self, $kernel, $heap) = @_[OBJECT, KERNEL, HEAP];
   
   return if $heap->{is_a_child} && !$heap->{can_fork};
   
   while (scalar(keys %{$heap->{children}}) < $heap->{processes}) {
      my $pid = fork();
      
      unless (defined($pid)) {
         $kernel->delay(do_fork => 1);
         return;
      }
      
      if ($pid) {
         $heap->{server_wheel}->pause_accept;
         $heap->{children}->{$pid} = 1;
         $kernel->sig_child($pid, "got_sig_child");
         next;
      }
      
      
      $heap->{connections_per_child}->{$$} = 0;
      $heap->{active_connections}->{$$} = 0;
   
      $heap->{server_wheel}->resume_accept;
      $kernel->has_forked();
      $heap->{is_a_child} = 1;
      $heap->{children}   = {};
      $kernel->yield('ticks');
      return;
   }
}

sub server_stop {
   my ($self, $kernel, $heap) = @_[OBJECT, KERNEL, HEAP];
   
}

sub server_accept_success {
   my ($self, $heap, $socket, $peer_addr, $peer_port) = @_[OBJECT, HEAP, ARG0, ARG1, ARG2];
   
   my $forwarder = POE::Component::Proxy::MySQL::Forwarder->new({
      socket         => $socket, 
      peer_addr      => $peer_addr, 
      peer_port      => $peer_port, 
      remote_addr    => $heap->{remote_addr}, 
      remote_port    => $heap->{remote_port}, 
      heap           => $heap,
      namespace      => ref($self),
   });

}

sub server_accept_failure {
  my ($heap, $operation, $errnum, $errstr) = @_[HEAP, ARG0, ARG1, ARG2];

  delete $heap->{server_wheel} if $errnum == ENFILE or $errnum == EMFILE;
}

sub run {
   POE::Kernel->run();
}

=head1 NAME

POE::Component::Proxy::MySQL - A POE MySQL proxy

=head1 DESCRIPTION

This modules helps building a MySQL proxy in which you can write
handler to deal with specific queries. It uses Moose and POE.

=head1 SYNOPSYS

First you create a server class that extends POE::Component::Proxy::MySQL.

   package MyMySQL;
   use Moose;
   
   extends 'POE::Component::Proxy::MySQL';

Then in a perl script you can instantiate your new server

   use MyMySQL;
   my $server = MyMySQL->new_with_options();
   $server->run;

In the MyMySQL namespace you can add roles which will act as handlers
for your trapped queries:

   package MyMySQL::Fortune;
   use MooseX::MethodAttributes::Role;
   
   sub fortune : Regexp('qr{fortune}io') {
      my ($self) = @_;
      
   	my $fortune = `fortune`;
   	chomp($fortune);
   
      $self->send_results(['fortune'],[[$fortune]]);
   
   }

=head1 AUTHORS

Eriam Schaffter, C<eriam@cpan.org>.

=head1 BUGS

None that I know of.

=head1 COPYRIGHT

This program is free software, you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut



1;

