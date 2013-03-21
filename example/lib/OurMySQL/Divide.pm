package OurMySQL::Divide;

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
use ProxyCursor;
use Template::Extract;
use DB;

use Sys::Hostname;

has 'batch_id'          => ( isa  => 'Str', is => 'rw' );
has 'dest_table'        => ( isa  => 'Str', is => 'rw' );
has 'wants_ok'          => ( isa  => 'Bool', is => 'rw', default => 0 );

has 'query'             => ( isa  => 'Str', is => 'rw' );
has 'cursor_query'      => ( isa  => 'Str', is => 'rw' );

has 'lib_name'          => ( isa  => 'Str', is => 'rw' );
has 'inc'               => ( isa  => 'Str', is => 'rw' );

has 'dt_loop'           => ( isa  => 'Str', is => 'rw' );
has 'deb'               => ( isa  => 'Str', is => 'rw' );
has 'fin'               => ( isa  => 'Str', is => 'rw' );

has 'timeout'           => ( isa  => 'Str', is => 'rw' );
has 'batch_val_prec'    => ( isa  => 'Str', is => 'rw' );

has 'memd'              => (is => 'rw', isa => 'Any');
has 'memd_lock'         => (is => 'rw', isa => 'Any');

has 'worker'            => (is => 'rw', isa => 'Any');

sub and_conquer : Regexp('qr{proxy_cursor}io') { #' "
   my ($self, $heap, $session, $kernel, $query, $tmp_placeholders) = 
      @_[OBJECT, HEAP, SESSION, KERNEL,, ARG0, ARG1];

   my $db = DB->new;

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



   my $dest_table;
   my $dest_schema;
   
   my $bulk_query;
   
   my $divide_schema;
   my $divide_table;
   my $divide_request;
   
   
   
   $query =~ s/with proxy_cursor on (.*)//g;
   $divide_table = $1;

   if ($divide_table) {
      ($divide_schema, $divide_table) = split(/\./, $divide_table);
#      print '$divide_schema = '.$divide_schema."\n";
#      print '$divide_table = '.$divide_table."\n";
   }
      
   my $table = $db->schema_mysql->resultset('Tables')->find({
      TABLE_NAME     => $divide_table,
      TABLE_SCHEMA   => $divide_schema,
   });      
   
   my $cursor_query;
   
   if ($table) {
      $cursor_query = 'SELECT * FROM '.$divide_schema.'.'.$divide_table
   }
   else {
      
      $query =~ s/with proxy_cursor on \((.*)\)//g;
      $divide_request = $1;
      
      if ($divide_request) {
#         print '$divide_request = '.$divide_request."\n";
      }
   
      $cursor_query = $divide_request;
   }
   
   if ($query =~ /insert into (.*) \(/i || $query =~ /insert into (.*) select/i) {
      $dest_table = $1;
      ($dest_schema, $dest_table) = split(/\./, $dest_table);
      
      $self->dest_table($dest_schema.'.'.$dest_table);
      
#      print '$dest_schema = '.$dest_schema."\n";
#      print '$dest_table = '.$dest_table."\n";
   
      $bulk_query = $query;
      
      $bulk_query =~ s/\r/ /ig;
      $bulk_query =~ s/\n/ /ig;
   
      use Template::Extract;
      use Data::Dumper;
      
      my $obj = Template::Extract->new;
      my $template = "insert into [% table_name %] ([% columns %]) [% select %]";
      my $data = $obj->extract($template, $bulk_query);
      
      $bulk_query = $data->{select};
      
      $self->dest_table($data->{table_name});
      
#      print '$query = '.$query."\n";
#      print '$bulk_query = '.$bulk_query."\n";
     
      
      $self->wants_ok(1);
      
   }
   else {
      $self->dest_table('dev_ers.'.sha1_hex(time.$$));
      
      $bulk_query = $query;
      
      $self->wants_ok(0);
      
   }

   $cursor_query =~ s/^\(//ig;
   $cursor_query =~ s/\)$//ig;
      
   
   $self->query($bulk_query);

   $self->cursor_query($cursor_query);


   my $create_table_query = $bulk_query;
   $create_table_query =~ s/where/where 1=0 and/g;
   $create_table_query = 'create table if not exists '.$self->dest_table.' as '.$create_table_query.';';
   
   print "Chargement de ".$self->dest_table." en mode dispatch \n";

   $self->server_send_query({
      query    => $create_table_query,
      callback => 'create_table',
   });

}






sub create_table {
   my ($self, $heap, $session, $kernel, $array) = @_[OBJECT, HEAP, SESSION, KERNEL, ARG0];

#   $self->client_send_ok;
   
   $self->server_send_query({
      query    => $self->cursor_query,
      callback => 'dispatch_on_table',
   });


}




sub dispatch_on_table {
   my ($self, $heap, $session, $kernel, $array) = @_[OBJECT, HEAP, SESSION, KERNEL, ARG0];

   my $columns = shift @{$array};
      
   $self->batch_id(sha1_hex(time.$$));
   
   $self->lib_name('ProxyCursor');
   
   $self->inc('Bin/OurMySQL/lib');

   my $args = ();
   
   my $host = hostname;
   
   
   $args->{'lib_name'}     = $self->lib_name;
   $args->{'inc'}          = $self->inc;
   $args->{'batch_id'}     = $self->batch_id;
   
   my $pc = ProxyCursor->new($args);
   
   
#   if (@array) {
      
      
      foreach my $data (@$array) {
         next if ref($data) ne 'ARRAY';
         next if $data->[0] eq 'source';
         
         my $template = $self->query;
         
         my $i = 0;         
         foreach my $column (@$columns) {
            my $value = $args->{$column} = $data->[$i++];
            $template =~ s/\@$column/$value/g;
         }
         
         $args->{'query'}        = $template;
         $args->{'dest_table'}   = $self->dest_table;
         $args->{'action'}      = 'run_'.$host;
         
         $pc->dispatch($args);
      
      }
      
      $kernel->delay('OurMySQL_Divide_poll_results' => 2);

}


sub poll_results {
   my ($self, $heap, $session, $kernel) = @_[OBJECT, HEAP, SESSION, KERNEL];
   
   if($self->memd_lock->lock("lock_".$self->batch_id)) {

      my $batch_val_n = $self->memd->get($self->batch_id);

      print "Batch ".$self->batch_id." val ".$batch_val_n." \n";

      no warnings;
      
      if ($batch_val_n <= -1 || !defined $batch_val_n) {
         
         $self->memd->delete($self->batch_id);
         $self->memd->delete("lock_".$self->batch_id);
         
         print "Batch ".$self->batch_id." completed \n";
                  
         $self->client_send_ok;
         
         
#         if ($self->wants_ok) {
#            $self->client_send_ok;
#         }
#         else {
#            $self->server_send_query({
#               query    => 'select * from '.$self->dest_table,
#            });
#         }
	      
      }
      else {
         
         $self->memd_lock->unlock;
         
         $kernel->delay(OurMySQL_Divide_poll_results => 2);
         
      }
      
   }
   else {
      $kernel->delay(OurMySQL_Divide_poll_results => 2);
   }

}

1;
