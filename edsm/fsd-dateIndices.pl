#!/usr/bin/perl

use strict;
use warnings;
use JSON::XS;
use ZMQ::FFI qw(ZMQ_SUB);
use Time::HiRes qw( usleep ualarm gettimeofday tv_interval);
use Compress::Zlib;
use HTTP::Request;
use LWP::UserAgent;
use LWP::Protocol::https;
use Date::Calc qw(Week_Number); 


my $esHostname = "127.0.0.1";
my $esPort = 9200;
#my $esUsername = "elastic";
#my $esPassword="elastic";
my $esUsername = "elastic";
#my $esPassword="ydQJZDL7lD0SzcuDWUNhOE9o";
my $esPassword="AoEPoc6IWNfr5jBYjGHpe3EF";

my $esScheme = "http";
my $useCert = "no";  ## should we use a client certficate for auth?
my $indexPrefix = "edsm-";

my $timestart = localtime();
my $counter = 0;
my $limit = 1000;

my $batchSize = 10;

my $endpoint = "tcp://eddn.edcd.io:9500"; ## Incoming ZMQ data

#my $esEndpoint = "http://192.168.0.112:9200/";
#my $esEndpoint = $esScheme."://".$esHostname.":".$esPort."/";
#my $esEndpoint = "http://127.0.0.1:9200/";#fsd_jump/_doc";
#my $esEndpoint = "https://eba44d3d3d5f4f02b624f84222c46ca0.europe-west1.gcp.cloud.es.io:9243/";
my $esEndpoint = "https://d40b8389cbe74a7bbdd2fc27fcc2a118.us-east-1.aws.found.io:9243/";


#/_security/_authenticate;
my $ctx      = ZMQ::FFI->new();
my $ua = LWP::UserAgent->new;


$ua->ssl_opts( #$key => $value 
                 #   SSL_version         => 'SSLv3',
                    SSL_ca_file         => '/Dump/data/testdata/certstest/elasticsearch/ca.crt',
                    #SSL_passwd_cb       => sub { return "xxxxx\n"; },
                    SSL_cert_file       => '/Dump/data/testdata/certstest/elasticsearch/andy.pem',
                    SSL_key_file        => '/Dump/data/testdata/certstest/elasticsearch/andy.key',
                    verify_hostname => 0 ,
                    SSL_verify_mode => IO::Socket::SSL::SSL_VERIFY_NONE,
                  
                ); # 
                

                
# $ua->credentials($esEndpoint, 'security', 'elastic', 'aZZ3Mp03CyoZLs5WDxZKJ9BU');    
#$ua->authorization_basic('elastic', 'elastic');   
# my $req = HTTP::Request->new(GET => $esEndpoint."_security/_authenticate?user=elastic&password=aZZ3Mp03CyoZLs5WDxZKJ9BU");
# #$req->authorization_basic($esEndpoint, 'security','elastic','aZZ3Mp03CyoZLs5WDxZKJ9BU');
# my $resp =  $ua->request($req);



# print $resp->code."\n";
# print $resp->content."\n";

my %schemas;
my %events;
my $indexName;
my $s = $ctx->socket(ZMQ_SUB);

$s->connect($endpoint);
my $req = HTTP::Request->new();
my $resp;
my $totalTimeSleeping=0;
my $totalTimeSending=0;
my $t0;
my $t1;
my $seconds0;
my $microseconds0;
my $seconds1;
my $microseconds1;
my $elapsed;

$s->subscribe('');
while($counter < $limit)
{
  $counter++;

   $t0 = [gettimeofday];
   # usleep 100_000 until ($s->has_pollin);
  $elapsed = tv_interval ( $t0, [gettimeofday]);
  $totalTimeSleeping=$totalTimeSleeping+$elapsed;
  print $elapsed."    -     ".$totalTimeSleeping."\n";


  #   ($seconds0, $microseconds0) = gettimeofday;
  # usleep 100_000 until ($s->has_pollin);
  #   ($seconds1, $microseconds1) = gettimeofday;
  #  $totalTimeSleeping+=$microseconds1-$microseconds0;
  my $data = $s->recv();
  # turn the json into a perl hash
  #	print OUTPUTFILE uncompress($data)."\n"; 

  my $pj = decode_json(uncompress($data));
  my $schema = $pj->{'$schemaRef'};
  $_ = $schema;
   my $event = $pj->{message}->{event};

  if (/eddn.edcd.io\/schemas\/(.*)\//)
  {
    my $thisSchema = $1;
    if (defined $event)
    {
      $indexName = $event;
    }
    else
    {
      $indexName = $thisSchema
    }
  }
  else 
  {
    die "Don't know which index to put this document into \n ".$pj."\n";
  }

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =  localtime();
  $year = $year+1900;
  $mon = $mon+1;
  my $wnum = Week_Number($year, $mon, $mday); 

  ### Hourly indices
  #$indexName = $indexName."-".$year."-".sprintf("%02d", $mon)."-".sprintf("%02d", $mday)."-".sprintf("%02d", $hour);

  if ($indexName eq "Scan")
  {
    ### Weekly indices
    $indexName = $indexPrefix.$indexName."-".$year."-w".sprintf("%02d", $wnum);
  }
  else
  {
    ### Monthly indices
    $indexName = $indexPrefix.$indexName."-".$year."-m".sprintf("%02d", $mon); 
  }




  for my $key ( keys %$pj ) 
  {
    if ( $key eq '$schemaRef') 
    {
      # print $key." -- ". $pj->{$key}. "\n";
    }
    else
    {
      for my $subKey (keys %{$pj->{$key}})
      {
         #print $subKey . "-- ". $pj->{$key}->{$subKey}."\n";
         $pj->{$subKey} =  $pj->{$key}->{$subKey};
         #  print "Parent: ". $pj->{$subKey}."\n";
      }
      delete $pj->{$key};
    }
  }

  ## Standardise "systemName" and "StarSystem" to "StarSystem"
  if (! defined  $pj->{StarSystem})
  {
    $pj->{StarSystem} = $pj->{systemName};
  }

  ## Normalise 3d Star co-ordinates, as Kibana doesn't cope very well with objects
  if (defined $pj->{StarPos})
  {
    $pj->{StarPosX} = $pj->{StarPos}[0];
    $pj->{StarPosY} = $pj->{StarPos}[1];
    $pj->{StarPosZ} = $pj->{StarPos}[2];
  }

  $data = encode_json($pj);
  #print ">>>>>>>>>>>>>>>>>>>>>>>\n$data\n <<<<<<<<<<<<<<<<<<<<\n";
  # print  "schema = " . $schema ."\n";
  # print "  software = ". $pj->{header}->{softwareName}."\n";
  # print   "  StarSystem = ". $pj->{message}->{StarSystem}."\n";


 
  $req = HTTP::Request->new(POST => $esEndpoint.lc $indexName."/_doc");
  if ($useCert ne 'yes')
  {
    $req->authorization_basic($esUsername,$esPassword);
  } 

  $req->header('content-type' => 'application/json');
  #$req->content( uncompress($data));
  $req->content( $data);

  $t0 = [gettimeofday];
  
  $resp = $ua->request($req);
  
  
  $elapsed = tv_interval ( $t0, [gettimeofday]);
  $totalTimeSending=$totalTimeSending+$elapsed;
  
  #print "Request: ".$req->as_string   ."\n";
  print    $indexName." -- ";
  if ( defined $pj->{StarSystem})
  {
    print $pj->{StarSystem};
  }
  else
  {
    print "SOMETHING ";
  } 
  print "  --- Elastic response: ". $resp->code." -- ".$resp->message."\n";
  #print "RESP: ".$resp->as_string ."\n";

  #print $resp->message."\n";


#  }
#   else 
#   {
#       print "Schema: ".$schema."\n";
#       die;
#   }
#   print "------\n";
}
print "Start Time: ".$timestart."\n";
print "End   Time: ".localtime()."\n";
print "Total events: ". $counter."\n";
print "Total time sleeping :".$totalTimeSleeping."\n";
print "Total time sending  :".$totalTimeSending."\n";

$s->unsubscribe('');
