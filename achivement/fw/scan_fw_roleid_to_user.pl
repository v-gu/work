#!/usr/bin/perl -I/apps/lib
#
# Implementation of reading live log files inserting into MySQL
# Written by Greg Heckenbach
# 2010-04-30
# Modified by Don for HOTK
# Modified by Arnold Domingo for Forsaken World (Done)
# 2011-01-28
#

use POSIX qw(setsid);
use File::Tail::Multi;
use Date::Manip;
use Log::Dispatch;
use Log::Dispatch::FileRotate;
use Proc::PID::File;
use IO::Socket;
use Encode qw/decode/;
use Time::HiRes qw(usleep gettimeofday);
use WWW::Curl::Easy;
use WWW::Curl::Form;
use Digest::MD5 qw(md5_hex);
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


# location for perfectworld log file 
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


# PWESSANDRA CONFIG VARIABLES
my $dbhost = "172.29.1.162";
my $dbport = "9160";
my $keyspace = "game_6";



my $cass = new CassandraPW4( $dbhost, $dbport, $keyspace );
$cass->set_consistency( # set consistency levels.  default for read/write is QUORUM
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM, # read
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM  # write
);


our $PROGRAM = $0; $PROGRAM =~ s|.*/||;

my $logDateD = Util::get_epoch2date(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";
`echo "START: $logDateD --- SERVER: $server --- FILE: $file" >> $logPath/$PROGRAM-server-$server`;

open(FILE, "<$file") or die "Can't open $file\n";
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

		if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:createrole:userid=(\d+):roleid=(\d+):city=(\d+):race=(\d+),occupation=(\d+):gender=(\d+)/) {
			# character is created: get role information
			my ($datetime, $userid, $roleid, $city, $race, $occupation, $gender) = ($1, $2, $3, $4, $5, $6, $7);
			insertRoleidToUserid ($datetime, $userid, $roleid, $server, $city, $race, $occupation, $gender);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) backup glinkd-\d+: notice : formatlog:rolelogin:userid=(\d+):roleid=(\d+)/) {
			# existing character logs into the game
			my ($datetime, $userid, $roleid) = ($1, $2, $3);
			insertRoleid ($datetime, $userid, $roleid, $server);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:getrole:sid=\d+:userid=(\d+):roleid=(\d+):timestamp=\d+:level=(\d+):exp=\d+:money=(\d+)/) {
			# character changes roles on the login screen
			my ($datetime, $userid, $roleid, $level, $money) = ($1, $2, $3, $4, $5);
			insertroleid ($server, $datetime, $roleid, $level, $money, $userid);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+gamedbd: notice : formatlog:putrole:sid=.+:userid=(\d+):roleid=(\d+):timestamp=.+:level=(\d+):exp=.+:money=(\d+)/) {
			# character changes roles on the login screen
			my ($datetime, $userid, $roleid, $level, $money) = ($1, $2, $3, $4, $5);
			my $flag=1;
			insertroleid ($server, $datetime, $roleid, $level, $money, $userid, $flag);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+gamed: notice : levelup,roleid=(\d+),level=(\d+),money=(\d+)/) {
			# character levels up
			my ($datetime, $roleid, $level, $money) = ($1, $2, $3, $4);
			insertroleid ($server, $datetime, $roleid, $level, $money);

		}
	}
}

# Lib functions
#
sub insertroleid {
	my ($iserver, $datetime, $roleid, $level, $money, $userid, $flag)=@_;
	my $roleidserver="$iserver-$roleid-$game";
	my ($iserial, $slastlogin, $slastlogout, $unserial, $smoney, $slevel);
	my %data = ();
	my %dataserial = ();
	my $msg ="Upgrade = money:$money , level:$level , ";
	%data = (	'money'	=>	"$money,$datetime", );
	$data{'level'} = "$level,$datetime" if (!$userid);
	my $time = $cass->get_timestamp($datetime);
	if ($userid) # rolelogin / logout
	{	
		#logout
		if ($flag)
		{
			$data{'lastlogout'} = $datetime;
			$msg .= "lastlogout ";
		} else # login
		{
			$data{'lastlogin'} = $datetime;
			$msg .= "lastlogin ";
		}
		$data{'userid'}=$userid;
		my %udata=(	"$roleidserver"	=> $datetime)	;
		$cass->set('user2roles',$userid,\%udata, $time);	

		$msg .= "set(user2roles, $userid =>$datetime , $time)";
	}

	$cass->set('roleinfo',$roleidserver,\%data, $time);
	$msg = "$msg -- set(roleinfo, $roleidserver =>$datetime , $time)";
	my $logDate = Util::get_epoch2date(time);
	print "$logDate $datetime $msg -- insertroleid \n";
}


#### NEED TO BE TAKEING OFF WHEN LIVE #####
sub insertRoleid {
	my $datetime = shift;
	my $userid = shift;
	my $roleid = shift;
	my $iserver = shift;
	my %data = ( 'userid' => $userid );
	my $time = $cass->get_timestamp($datetime);
	my $roleidserver="$iserver-$roleid-$game";
	my %udata=( "$roleidserver" => $datetime );

	$cass->set('roleinfo',$roleidserver,\%data,$time);
	$cass->set('user2roles',$userid,\%udata, $time);
	my $msg = "set(roleinfo,user2roles $roleidserver, $userid=>$datetime, $time)";
	my $logDate = Util::get_epoch2date(time);
	print "$logDate $datetime $msg  --  insertRoleid\n";
}

### EXCLUSIVE FOR CREATE ONLY ####
sub insertRoleidToUserid {
	my ($datetime, $userid, $roleid, $iserver, $city, $race, $occupation, $gender) = @_;
	# race :: 1 - human, 2 - elf, 3 - dwarf, 4 - stoneman, 5 - kindred
	# occupation :: 1 - warrior, 2 - protector, 3 - assassin, 4 - marksman , 5 - mage , 6 - priest , 7 - vampire , 8 - bard
	# 1 => 1,3,5,6 :: 2 => 1,6,8 :: 3 => 4 :: 4 => 2 :: 5 => 3,5,7
	
	my %data = (
		'userid' => $userid,
		'created' => $datetime,
		'occupation'	=> $occupation,
		'gender'	=> $gender,
		'city'	=>	$city,
		'race'	=> $race,
		'forumid'	=> '',
		'lastlogin'	=> $datetime,
		'lastlogout'	=> '',
		'isActive'	=> 1,
		'level'	=> "1,$datetime",
		'total_time'	=> '',
		'pvpkills'	=> '',
		'killsUpdate'	=> '',
		'guild'	=>	'',
		'deleted'	=>	'',
		'money'			=> '',
		'lastguildjoin' => '',
	);
	my $roleidserver="$iserver-$roleid-$game";
	my $time = $cass->get_timestamp($datetime);
	my $logDate = Util::get_epoch2date(time);
	my $msg = "userid:$userid , occ:$occupation , gender:$gender";
	print "$logDate $datetime $msg -- char created\n";
	$cass->set('roleinfo',$roleidserver,\%data,$time);
	$msg = "set(roleinfo, $roleidserver, $userid=>$datetime)";
	print "$logDate $datetime $msg -- insertRoleidToUserid\n";
	my %udata = ( $roleidserver => $datetime, );
	$cass->set('user2roles',$userid,\%udata,$time);
	$msg = "set(user2roles, $userid=>created = $datetime)";
	print "$logDate $datetime $msg -- insertRoleidToUserid\n";
	
	insert2DB ($datetime, $cass, 'roleinfo', "$iserver-$roleid-$game-stat", $datetime, "created,$userid,$race,$occupation,$gender,$city");
#	die if ($roleid eq 6602753);
}

sub insert2DB {
	my ($datetime, $cassvar, $colfamily, $key, $colname, $colval) = @_;
	my %hhash = ($colname	=>	$colval 	);
	my $time = $cassvar->get_timestamp($datetime);
	my $logDate = Util::get_epoch2date(time);
	$cassvar->set($colfamily, $key, \%hhash, $time);
	my $msg = "set($colfamily, $key, $colname => $colval)";
	print "$logDate $datetime $msg\n";
}
