#!/usr/bin/perl

use strict;
use warnings;
use JSON::XS;#  qw(decode_json, encode_json);
use ZMQ::FFI qw(ZMQ_SUB);
use Time::HiRes q(usleep);
use Compress::Zlib;
use HTTP::Request;
use LWP::UserAgent;
use LWP::Protocol::https;



#$IO::Socket::SSL::DEBUG=2;

my $esHostname = "127.0.0.1";
my $esPort = 9200;
my $esUsername = "elastic";
my $esPassword="elastic";
my $esScheme = "http";
my $useCert = "no";  ## should we use a client certficate for auth?

### Parameters (must be in this order )
# hostname
# port
# username
# password
if ( defined $ARGV[0])
{
  $esHostname = $ARGV[0];
  if ( defined $ARGV[1])
  {
    $esPort = $ARGV[1];
    if ( defined $ARGV[2])
      {
        $esUsername = $ARGV[2];
        if ( defined $ARGV[3])
        {
          $esPassword = $ARGV[3];
          if ( defined $ARGV[4])
          {
            $esScheme = $ARGV[4];
            if ( defined $ARGV[5])
            {
              $useCert = $ARGV[5];
            }
          }
        }
      }
  }
}

my $endpoint = "tcp://eddn.edcd.io:9500";
#my $esEndpoint = "http://192.168.0.112:9200/";#/fsd_jump/_doc";


my $esEndpoint = $esScheme."://".$esHostname.":".$esPort."/";#fsd_jump/_doc";
#my $esEndpoint = "http://127.0.0.1:9200/";#fsd_jump/_doc";
#my $esEndpoint = "https://eba44d3d3d5f4f02b624f84222c46ca0.europe-west1.gcp.cloud.es.io:9243/";

#/_security/_authenticate;
my $ctx      = ZMQ::FFI->new();
my $ua = LWP::UserAgent->new;


$ua->ssl_opts( #$key => $value 
                 #   SSL_version         => 'SSLv3',
                    SSL_ca_file         => '/Dump/data/testdata/certstest/elasticsearch/ca.crt',
                    #SSL_passwd_cb       => sub { return "xxxxx\n"; },
                    SSL_cert_file       => '/Dump/data/testdata/certstest/elasticsearch/andy.pem',
                    SSL_key_file        => '/Dump/data/testdata/certstest/elasticsearch/andy.key',
                ); # ssl_opts => { verify_hostname => 0 }

                
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

$s->subscribe('');
while(1)
{
  usleep 100_000 until ($s->has_pollin);
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




for my $key ( keys %$pj ) 
{
  if ( $key eq '$schemaRef') {
  # print $key." -- ". $pj->{$key}. "\n";
}
else
{
  for my $subKey (keys %{$pj->{$key}})
  {
 #   print $subKey . "-- ". $pj->{$key}->{$subKey}."\n";
  

  
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
$resp = $ua->request($req);
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
$s->unsubscribe('');
