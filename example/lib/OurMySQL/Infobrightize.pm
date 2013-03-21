package OurMySQL::Infobrightize;

use MooseX::MethodAttributes::Role;

use POE;

use Data::Dumper;
use SQL::Translator;
use POSIX qw/ceil/;
use Digest::SHA1  qw(sha1_hex);

has 'source'         => (is => 'rw', isa => 'Str');
has 'destination'    => (is => 'rw', isa => 'Str');
has 'schema_name'    => (is => 'rw', isa => 'Str');
has 'table_name'     => (is => 'rw', isa => 'Str');
has 'table_fqn'      => (is => 'rw', isa => 'Str');


sub BUILD {
   print "OurMySQL::Infobrightize->BUILD \n";
}

sub infobrightize : Regexp('qr{(INFOBRIGHTIZE(.*)\((.*))}io') { #' "
   my ($self, $heap, $session, $query, $tmp_placeholders) = 
      @_[OBJECT, HEAP, SESSION, ARG0, ARG1];
   
   my @placeholders = @{$tmp_placeholders};
   
#   $self->release_client;
#   $self->release_server;
   
#   my $table_name = $placeholders[2];
#   my $db_name;
   
   my ($db_name, $table_name, $destination) = $self->_extract_from_infobright_query($query);
   
   $self->destination($destination) if defined $destination;
   $self->schema_name($db_name);
   $self->table_name($table_name);
   
#   ($db_name, $table_name) = split (/\./, $table_name) if $table_name =~ /(.*)\.(.*)/;
   
#   print 'infobrightize '.$db_name.".".$table_name.(defined $destination ? "to ".$destination : '')."\n";
   
   $heap->{'db_name'} = $db_name;
   $heap->{'table_name'} = $table_name;
   
   chomp($table_name);
   
   
   $self->table_fqn($db_name ? $db_name.'.'.$table_name : $table_name);
   
   
#   print 'table_fqn = '.$self->table_fqn."\n";
   
#   $heap->{'table'} = $heap->{'db_name'} ? $heap->{'db_name'}.'.'.$heap->{'table_name'} : $heap->{'table_name'};
   
   $self->server_send_query({
      query    => "select * from ".$self->table_fqn." limit 1",
      callback => 'select_limit_1',
   });
   
}





sub select_limit_1 {
   my ($self, $heap, $session, $tmp_array) = @_[OBJECT, HEAP, SESSION, ARG0];

#   print "select_limit_1 \n";

#   print Dumper($tmp_array);

   my @array = @{$tmp_array};
   
   if (@array) {
      
      my $destination = $self->destination;
      
      $self->server_send_query({
         query    => "drop table if exists ".$destination." ",
         callback => 'drop_table',
      });
      
   }
   else {
      $self->client_send_error('Table does not exists !');
   }

}


sub drop_table {
   my ($self, $heap, $session, $tmp_array) = @_[OBJECT, HEAP, SESSION, ARG0];

#   print "drop_table \n";
   my @array = @{$tmp_array};

#   print Dumper($tmp_array);
   
#   print "show create table ".$self->table_fqn."\n";

   $self->server_send_query({
      query    => "show create table ".$self->table_fqn,
      callback => 'show_create_table',
   });
}


sub show_create_table {
   my ($self, $heap, $session, $tmp_array, $input) = @_[OBJECT, HEAP, SESSION, ARG0, ARG1];

#   print "show_create_table \n";
#   
#   print Dumper($tmp_array);
   
   my @array = @{$tmp_array};

      
      my $create_table = $array[1][1].";";
#      $create_table =~  s/`//g;
   
      if ($create_table =~ /BRIGHTHOUSE/i) {
         $heap->{'source_infobright'} = 1;
      }   
      
#      print $create_table."\n";
      
   
      my $t = SQL::Translator->new(        
         show_warnings     => 1,
         no_comments       => 1,      
         quote_table_names => 1,
         quote_field_names => 1,
         parser            => 'MySQL',
         producer          => 'MySQL',
      );
      
      $t->filters( \&filter) or die $t->error;
   
      my @creates = $t->translate( \$create_table );
      $create_table = $creates[1];   
      
#      print '$create_table = '.$create_table."\n";
      
      $heap->{'charset'} = '';
      
      if ($create_table =~ /DEFAULT CHARACTER SET (.*) COLLATE/) {
         $heap->{'charset'} = $1;
      }
      if ($create_table =~ /DEFAULT CHARACTER SET (.*)$/ && $heap->{'charset'} eq '') {
         $heap->{'charset'} = $1;
      }
      
      if ($create_table =~ /DEFAULT CHARSET (.*) COLLATE/) {
         $heap->{'charset'} = $1;
      }
      if ($create_table =~ /DEFAULT CHARSET (.*)$/ && $heap->{'charset'} eq '') {
         $heap->{'charset'} = $1;
      }
      
      my $dbname = $heap->{db_name};
      
   #   $create_table =~ s/CREATE TABLE /CREATE TABLE $dbname\./g;
      
#      print '$create_table = '.$create_table."\n";
#      print 'charset = '.$heap->{'charset'}."\n";
   
      my $destination = $self->destination;
   
      $create_table =~ s/`~dest~`/$destination/g;
   
#      print '$create_table = '.$create_table."\n";
   
      $self->server_send_query({
         query    => $create_table,
         callback => 'create_table',
      });
#  }
}


sub create_table {
   my ($self, $heap, $session, $tmp_array) = @_[OBJECT, HEAP, SESSION, ARG0];

#   print "create_table \n";

   my $filename = sha1_hex($self->table_fqn.$$);
   $heap->{'filename'} = $filename;
   
   my $charset = '';
   $charset = 'character set '.$heap->{'charset'} if $heap->{'charset'} ne '';
#   print $heap->{'filename'}."\n";
   
   my $enclosed = '';
   $enclosed = 'NULL' if exists $heap->{'source_infobright'};

   
   my $query = "
      select *
      into outfile \'/home/Impact/simeco/tmp/ib/".${filename}.".dat\'
      ".$charset."
      fields terminated by \'~\' enclosed by \'".$enclosed."\'
      lines terminated by \'\\n\'
      from ".$self->table_fqn."; ";

#   print $query."\n";

   $self->server_send_query({
      query    => $query,
      callback => 'select_outfile',
   });
}


sub select_outfile {
   my ($self, $heap, $session, $tmp_array, $result) = @_[OBJECT, HEAP, SESSION, ARG0, ARG1];
   
   
#   print Dumper($tmp_array);
#   print Dumper($result);
   
#   print "select_outfile \n";
   my $filename = $heap->{'filename'};
   
   my $destination = $self->destination;
   
   my $charset = 'character set '.$heap->{'charset'} if $heap->{'charset'} ne '';
   
   my $query = "
      load data infile '/home/Impact/simeco/tmp/ib/".$filename.".dat'
      into table ".$destination."
      ".$charset."
      fields terminated by '~' 
      enclosed by 'NULL'; ";
   
#   print $query." \n";
   
   $self->server_send_query({
      query  => $query,
      callback => 'load_infile',
   });
}


sub load_infile {
   my ($self, $heap, $session, $tmp_array) = @_[OBJECT, HEAP, SESSION, ARG0];
   
#   print "load_infile \n";
   unlink "/home/Impact/simeco/tmp/ib/".$heap->{'filename'}.".dat";
   
   my $query = "select count(*) as '".$self->destination."' from ".$self->destination.";";  ;
   
   $self->server_send_query({
      query  => $query,
   });
}

sub _extract_from_infobright_query {
   my ($self, $query) = @_;
   
   $query =~ s/"//g; #"
   $query =~ s/'//g; #'
   $query =~ s/ //g; #'
   
   my $schema_name;
   my $table_name;
   my $destination;
   
   if ($query =~ /.*\((.*),(.*)\)/) {
      $schema_name   = $1;
      $destination   = $2;
   }
   elsif ($query =~ /.*\((.*)\)/) {
      $schema_name   = $1;
   }
   else {
      $self->client_send_error('You need to specify the database prefix');
   }
   
   if ($schema_name =~ /\./) {
      ($schema_name, $table_name) = split(/\./, $schema_name);
   }
   else {
      $table_name    = $schema_name;
      $schema_name   = '';
   }

   $self->schema_name('');
   $self->table_name('');
   $self->destination('');
   $self->table_fqn('');
   
   unless (defined $destination) {
      $destination = $schema_name.'.'.$table_name.'_ib';
   }

#   print 'schema_name   = '.$schema_name."\n";
#   print 'table_name    = '.$table_name."\n";
#   print 'destination   = '.$destination."\n";
      
   return ($schema_name, $table_name, $destination);
}

sub filter {        
   my $schema = shift;

   for my $table ( $schema->get_tables ) {

      foreach my $name ($table->get_constraints) {
         $table->drop_constraint($name);
      }
      
      foreach my $name ($table->get_indices) {
         $table->drop_index($name);
      }
      
      my @fields = $table->get_fields;

      foreach my $field (@fields) {         
         my $data_type = $field->data_type;
         
         
         $field->is_auto_increment(0);
#         $field->is_nullable(0);
#         $field->default_value('');
         
         $field->extra( 'unsigned' => 0 );
         $field->extra( 'character set' => 0 );
         $field->extra( 'collate' => 0 );
#         $field->extra( 'default' => '' );
#         $field->extra( 'not null' => 0 );
         
         if ($data_type eq 'double') {
            $field->data_type('double');
#            $field->size(qw/20 20/);
         }
         
         if ($data_type eq 'float') {
            $field->data_type('double');
#            $field->size(qw/20 20/);
         }
         
         if ($data_type eq 'enum') {
            $field->data_type('varchar');
            $field->size(255);
         }
         
         if ($data_type eq 'char') {
            $field->data_type('varchar');
            $field->size(255);
         }
         
         if ($data_type eq 'longtext') {
            $field->data_type('varchar');
            $field->size(255);
         }
         
         if ($data_type eq 'blob') {
            $field->data_type('varchar');
            $field->size(255);
         }
         
         
         if ($data_type eq 'decimal') {
            
            my ($dig, $dec) = split(/,/, $field->size);
            
            if ($dig > 18) {
               $dec = ceil($dec / $dig * 18);
               $dig = 18;
            }
            
            $field->data_type('decimal');
            
            $field->size([$dig, $dec]);
            
         }
      }

#      $table->name($table->name.'_ib');   
      $table->name('~dest~');      
  
      foreach my $option (@{$table->{'options'}}) {
         if (exists $option->{'ENGINE'}) {
            $option->{'ENGINE'} = 'brighthouse';
         }
      }

   }

}


1;
