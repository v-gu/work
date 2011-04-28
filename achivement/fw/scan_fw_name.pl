#!/usr/bin/perl -I/apps/lib

# Implementation of reading live log files inserting into MySQL
# Written by Greg Heckenbach
# 2010-04-30

use POSIX qw(setsid);
use File::Tail::Multi;
use Date::Manip;
use Log::Dispatch;
use Log::Dispatch::FileRotate;
use Proc::PID::File;
use IO::Socket;
use Encode qw/decode/;
use Time::HiRes qw(usleep gettimeofday);

use Config::Tiny;
use CassandraPW;
use CassandraPW4;
use CassandraPW_Time;
use Util;
use strict;
use warnings;

#####################################
#  configuration variables          #
#####################################
# server ids this script is using
#


die "Usage file.pl <file> <server>\n" if ($#ARGV != 1);

my ($file, $server)=@ARGV;

my %hserver = (
                    39   =>  'fw',
                    42   =>  'fw',
                    44   =>  'fw',
                    46   =>  'fw',
                    47   =>  'fw',
                    48   =>  'fw',
                    49   =>  'fw',
              );

my $game = $hserver{$server};
die "Server and game don't match!!\n" if (!$game);
my $logPath = "/data2/logs/$game/";

my $dbhost = "172.29.1.162";
my $dbport = "9160";
my $keyspace = "game_6";

# Connect to the database.
my $cass = new CassandraPW( $dbhost, $dbport, $keyspace );
$cass->set_consistency(	# set consistency levels. default for read/write is QUORUM
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM,	# read
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM	# write
);

our $PROGRAM = $0; $PROGRAM =~ s|.*/||;

my $logDateD = Util::get_epoch2date(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";
`echo "START: $logDateD --- SERVER: $server --- FILE: $file" >> $logPath/$PROGRAM-server-$server`;

open(FILE, "gunzip -c $file |") or die "Can't open $file\n";
my %data=();
while (<FILE>)
{
    chomp;
    process_data([$_]);
}

$logDateD = Util::get_epoch2date(time);
print "END: $logDateD --- SERVER: $server --- FILE: $file\n";
`echo "END: $logDateD --- SERVER: $server --- FILE: $file" >> $logPath/$PROGRAM-server-$server`;

sub process_data {
	my $lines_ref = shift;
	foreach ( @{$lines_ref} ) {
		chomp;
		my $str = $_;
		if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) backup unamed: debug : unamed::ConfirmName: result=\d+,zoneid=(\d+),category=\d+,id=(\d+),name="([^"]+)"/) {
			# Getting a Guild Name  --arnold.domingo
			my ($datetime, $zoneid, $kingdomid, $guildname) = ($1, $2, $3, $4);
			insertName($zoneid, $datetime, $kingdomid, $guildname, 'guild', 'guildname');
		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) backup unamed: debug : unamed::ConfirmRoleName: result=\d+,zoneid=(\d+),userid=\d+,roleid=(\d+),rolename="([^"]+)"/) {
			# Getting a Role Name  --arnold.domingo
			my ($datetime, $zoneid, $roleid, $rolename) = ($1, $2, $3, $4);
			insertName($zoneid, $datetime, $roleid, $rolename, 'roleinfo', 'rolename');
		}
	}
}


# Lib functions
#

sub insertName {
	my ($iserver, $datetime, $key, $name, $col1, $col2)=@_;
	my $ksg="$iserver-$key-$game";
	my $time = $cass->get_timestamp($datetime);

	my %data = ( 'name' => $name, );
	$cass->set($col1,$ksg,\%data,$time);

	$name =~ s/\0//g;
	$name =~ tr/[A-Z]/[a-z]/;
	my %dataname = ( $ksg => 1, );
	$cass->set($col2,$name,\%dataname,$time);
	
	if ($col2 ne 'rolename') {
		my %index = ( "$name-$iserver" => $ksg );
		$cass->set('index',$col2,\%index,$time);
	}

	my $msg = "set $col1($ksg:name=>$name) ; set $col2($name:$ksg=>1); time:$time";
	my $logDate = Util::get_epoch2date(time);
	print "$logDate $datetime => $msg\n";
}
