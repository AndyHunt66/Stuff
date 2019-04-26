#!/usr/bin/perl

use strict;
use warnings;
use JSON::XS 'decode_json';
use ZMQ::FFI qw(ZMQ_SUB);
use Time::HiRes q(usleep);
use Compress::Zlib;
use HTTP::Request;
use LWP::UserAgent;



my $endpoint = "tcp://eddn.edcd.io:9500";
my $esEndpoint = "http://192.168.0.100:9200/";#fsd_jump/_doc";
my $ctx      = ZMQ::FFI->new();
my $ua = LWP::UserAgent->new;

my $s = $ctx->socket(ZMQ_SUB);

$s->connect($endpoint);

$s->subscribe('');
while(1)
{
  usleep 100_000 until ($s->has_pollin);
  my $data = $s->recv();
  # turn the json into a perl hash
  #	print OUTPUTFILE uncompress($data)."\n"; 

  my $pj = decode_json(uncompress($data));
  my $schema = $pj->{'$schemaRef'};
#   if ($schema eq "https://eddn.edcd.io/schemas/journal/1") {
     my $event = $pj->{message}->{event};
    
 # print  "schema = " . $schema ."\n";
  print "  software = ". $pj->{header}->{softwareName}."\n";
  print   "  StarSystem = ". $pj->{message}->{StarSystem}."\n";
my $req = HTTP::Request->new(POST => $esEndpoint.lc $event."/_doc");

$req->header('content-type' => 'application/json');
$req->content( uncompress($data));
my $resp = $ua->request($req);
print "Request: ".$req->as_string   ."\n";
print "Elastic response: ". $resp->code."\n";
print $resp->message."\n";


#  }
#   else 
#   {
#       print "Schema: ".$schema."\n";
#       die;
#   }
#   print "------\n";
}
$s->unsubscribe('');
