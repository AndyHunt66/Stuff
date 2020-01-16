#!/usr/bin/perl

use strict;
use warnings;
#use JSON 'decode_json';
use JSON::PP qw(decode_json);
use ZMQ::FFI qw(ZMQ_SUB);
use Time::HiRes q(usleep);
use Compress::Zlib;
use HTTP::Request;
use LWP::UserAgent;
use Term::ReadKey;
use Data::Dumper;


#use JSON::PP qw(decode_json);
$JSON::PP::true  = 'true';
$JSON::PP::false = 'false';
$Data::Dumper::Indent = 0;
#$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;

my $endpoint = "tcp://eddn.edcd.io:9500";
#my $esEndpoint = "http://192.168.0.112:9200/";#/fsd_jump/_doc";
my $esEndpoint = "https://32f6567653a441ed9620cbd181ccc626.35.205.101.137.ip.es.io:9243/";#fsd_jump/_doc";
my $esUsername = "elastic";
my $esPassword = "tn2tUyza80VZS4QCqLxep1Ju";
#my $esEndpoint = "https://eba44d3d3d5f4f02b624f84222c46ca0.europe-west1.gcp.cloud.es.io:9243/";

my $outputFile = "./data.json";
open OUTPUTFILE, ">$outputFile" or die "Can't open outputfile $outputFile for writing - $! \n";

#/_security/_authenticate;
my $ctx      = ZMQ::FFI->new();
my $ua = LWP::UserAgent->new(
    ssl_opts => {
            SSL_verify_mode => 0,
            verify_hostname => 0
        }
);
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

ReadMode 4; # Turn off controls keys


$s->subscribe('');
my $key ;
while(not defined ( $key = ReadKey(-1)))
{
  usleep 100_000 until ($s->has_pollin);
  my $data = $s->recv();
  # turn the json into a perl hash
  #	print OUTPUTFILE uncompress($data)."\n"; 

  my $pj = decode_json(uncompress($data));
  #print OUTPUTFILE Dumper($pj)."\n";
#  print OUTPUTFILE $pj->{header}."\n";
#  print OUTPUTFILE $pj->{'$schemaRef'}."\n";
#  print OUTPUTFILE $pj->{message}."\n";
#  foreach my $key (keys %$pj) 
#  {
#      print $key."\n";
#  }
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

 # print  "schema = " . $schema ."\n";
 # print "  software = ". $pj->{header}->{softwareName}."\n";
 # print   "  StarSystem = ". $pj->{message}->{StarSystem}."\n";
 my $req = HTTP::Request->new(POST => $esEndpoint.lc $indexName."/_doc");
 
 
 $req->authorization_basic($esUsername,$esPassword);
#$req->authorization_basic('elastic','elastic');

$req->header('content-type' => 'application/json');
$req->content( uncompress($data));
my $resp = $ua->request($req);
#print "Request: ".$req->as_string   ."\n";
print "Elastic response: ". $resp->code."\n";
#print $resp->message."\n";


#  }
#   else 
#   {
#       print "Schema: ".$schema."\n";
#       die;
#   }
#   print "------\n";
}
print "Get key $key\n";
ReadMode 0; # Reset tty mode before exiting

$s->unsubscribe('');
close OUTPUTFILE;