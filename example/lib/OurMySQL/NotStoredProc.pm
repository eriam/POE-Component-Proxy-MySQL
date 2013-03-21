package OurMySQL::NotStoredProc;

use MooseX::MethodAttributes::Role;

use POE;
use Job;
use Data::Dumper;
use SQL::Translator;
use Class::Handle;
use Params;
use IPC::Lock::Memcached;
use Cache::Memcached::Fast;
use Digest::SHA1  qw(sha1_hex);
use OurMySQLMap;
use Template::Extract;

has 'batch_id'          => ( isa  => 'Str', is => 'rw' );
has 'dest_table'        => ( isa  => 'Str', is => 'rw' );

has 'args'              => ( isa  => 'HashRef', is => 'rw' );

has 'lib_name'          => ( isa  => 'Str', is => 'rw' );
has 'inc'               => ( isa  => 'Str', is => 'rw' );

has 'memd'              => (is => 'rw', isa => 'Any');
has 'memd_lock'         => (is => 'rw', isa => 'Any');

has 'worker'            => (is => 'rw', isa => 'Any');

#   query('Bin/Tests/lib/TestWorker', 'dev_ers.dest_table');
#   query('Bin/Tests/lib/TestWorker', 'dev_ers.dest_table');

sub not_stored_proc : Regexp('qr{(QUERY(.*)\((.*))\)}io') { #' "
   my ($self, $heap, $session, $kernel, $query, $tmp_placeholders) = 
      @_[OBJECT, HEAP, SESSION, KERNEL,, ARG0, ARG1];

   $self->memd(new Cache::Memcached::Fast({
      servers     => ["129.195.12.14:11211",],
   }));   

   $self->memd_lock(new IPC::Lock::Memcached({
     memcached_servers => ["129.195.12.14:11211",],
      ttl       => 86400,
      patience  => 60,
      increment => 1,
   }));   
   
   my @placeholders = @{$tmp_placeholders};
   
   my $document = $placeholders[2];
   $document .= "," unless substr($document, -1) eq ',';
      
   my $obj = Template::Extract->new;
   my $template = "[% FOREACH param %][% name %]=>[% value %],[% END %]";

   my $args = ();
   my $params = $obj->extract($template, $document);

   foreach (@{$params->{param}}) {
      my $p_name  = $_->{name};
      my $p_value = $_->{value};
      
      $p_name =~ s/ //g;
      
      if ($p_value =~ /(\"|\')(.*)(\"|\')/) {
         $p_value = $2;
      }
      else {
         $p_value =~ s/ //g;
      }
      
      $args->{$p_name} = $p_value;
   }
   
   my $pm_name = param_ini('LibDir').$args->{'class'}.".pm";
   
   $self->dest_table($args->{'dest_table'});
   
   if (-e $pm_name) {

      my (@inc) = split(/\//, $args->{'class'});
      
      my $lib_name = pop @inc;
      
      $self->lib_name($lib_name);
      
      $self->inc(join('/', @inc));
      
      push @INC, param_ini('LibDir').$self->inc;
      
      my $class = Class::Handle->new($self->lib_name);
   
      $class->load();
      
   }
   else {
      
      $self->lib_name('OurMySQLMap');
      
      $self->inc('Bin/OurMySQL/lib');
      
   }

   $self->batch_id(sha1_hex(time.$$));
   
   $args->{'lib_name'}  = $self->lib_name;
   $args->{'inc'}       = $self->inc;
   $args->{'jobs'}      = 1;
   $args->{'batch_id'}  = $self->batch_id;

   $self->args($args);
   
   $self->server_send_query({
      query    => "select * from ".$self->dest_table." limit 1",
      callback => 'table_is_there',
   });
   
}


sub table_is_there {
   my ($self, $heap, $session, $tmp_array) = @_[OBJECT, HEAP, SESSION, ARG0];

   my @array = @{$tmp_array};
   
   if (@array) {
      
#      print "Here be dragons ".$self->inc." - ".$self->lib_name."\n";
   
      $self->server_send_query({
         query    => 'truncate table '.$self->dest_table,
         callback => 'here_be_dragons',
      });
      
   }
   else {
      $self->client_send_error('Table does not exists !');
   }

}

sub here_be_dragons {
   my ($self, $heap, $session, $kernel) = @_[OBJECT, HEAP, SESSION, KERNEL];

   $self->worker($self->lib_name->new($self->args));

   my $args = $self->args;

   delete $args->{jobs};
   delete $args->{batch_id};
   delete $args->{lib_name};
   delete $args->{inc};
   delete $args->{dt_loop};

   $self->worker->lance($args);
      
   $kernel->delay('OurMySQL_NotStoredProc_poll_results' => 1);
 
}

sub poll_results {
   my ($self, $heap, $session, $kernel) = @_[OBJECT, HEAP, SESSION, KERNEL];
   
   if($self->memd_lock->lock("lock_".$self->batch_id)) {
   
      my $batch_val_n = $self->memd->get($self->batch_id);
            
#      print "Batch $$ ".$self->batch_id." val ".$batch_val_n." \n";
         
      no warnings;
      
      if ($batch_val_n <= -1) {
         
         print "Batch ".$self->batch_id." completed \n";

	      $self->server_send_query({
	         query    => 'select * from '.$self->dest_table,
	      });
	      
      }
      else {
         
         $self->memd_lock->unlock;
         
         $kernel->delay(OurMySQL_NotStoredProc_poll_results => 1);
         
      }
      
   }
   else {
      $kernel->delay(OurMySQL_NotStoredProc_poll_results => 1);
   }

}

1;
