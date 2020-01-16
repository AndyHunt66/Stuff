#!/usr/bin/perl

use strict;
use warnings;
use JSON::XS 'decode_json';
use ZMQ::FFI qw(ZMQ_SUB);
use Time::HiRes q(usleep);
use Compress::Zlib;
use HTTP::Request;
use LWP::UserAgent;
use Data::GUID;
use Term::ReadKey;



my $endpoint = "tcp://eddn.edcd.io:9500";

my $ctx      = ZMQ::FFI->new();
my $ua = LWP::UserAgent->new;

my $baseOutputFilename = "./output/";
my $outputFileName = $baseOutputFilename . "temp.json";


my %schemas;
my %events;
my $indexName;
my $s = $ctx->socket(ZMQ_SUB);

$s->connect($endpoint);

$s->subscribe('');
my $key ;
my $count = 0;
while(not defined ( $key = ReadKey(-1)))
{
    my $guid = Data::GUID->new;
    $outputFileName = $baseOutputFilename . $guid.".json";
    open OUTPUTFILE, ">$outputFileName" or die ;
  usleep 100_000 until ($s->has_pollin);
  my $data = $s->recv();
  # turn the json into a perl hash
  print OUTPUTFILE uncompress($data)."\n"; 
#  print   "  StarSystem = ". uncompress($data)."\n";
if ($count % 1000 eq 0)
{
    print   $count ."\n";
}

    close OUTPUTFILE ;
    $count++;
}

 # print  "schema = " . $schema ."\n";
 # print "  software = ". $pj->{header}->{softwareName}."\n";
 #my $req = HTTP::Request->new(POST => $esEndpoint.lc $indexName."/_doc");
 #$req->authorization_basic('elastic','elastic');

#$req->header('content-type' => 'application/json');
#$req->content( uncompress($data));
#my $resp = $ua->request($req);
#print "Request: ".$req->as_string   ."\n";
#print "Elastic response: ". $resp->code."\n";
#print $resp->message."\n";


#  }
#   else 
#   {
#       print "Schema: ".$schema."\n";
#       die;
#   }
#   print "------\n";
#}
$s->unsubscribe('');
