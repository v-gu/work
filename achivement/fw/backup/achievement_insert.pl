#!/usr/bin/perl -I/apps/lib

use Net::Cassandra;

use Switch;
use Time::HiRes qw(usleep gettimeofday);
use Data::Dumper;

use CassandraPW;
use strict;
use warnings;

my $file = "achieve-raw2.txt";
# CASS CONFIG VARIABLES
my $Chost   = "192.168.8.6";
my $Cport   = "9160";
my $keyspace = "game_5";

print "test\n";
# Connect to pwessandra
my $cass = new CassandraPW( $Chost, $Cport, $keyspace );
$cass->set_consistency( # set consistency levels.  default for read/write is ONE
        Net::Cassandra::Backend::ConsistencyLevel::ONE, # read
        Net::Cassandra::Backend::ConsistencyLevel::ONE  # write
);

# Read file

my $httpurl="http://hotk-forum.perfectworld.com/templates/common/images/avatars/BT";
my $scriptname="scan_hotk_pvp_x.pl";
my ($pointvalue, $name, $descr);

open (FILE, "<$file") or die "Can't open $file\n";

my $count = 1;

my $game = "Heroes of Three Kingdoms";

while (1)
{
	my %indexdata = ( $count	=>	$count,	);
	$cass->set('index', 'test', \%indexdata);
print Dumper \%indexdata;
sleep 5;
	$count++;
}
