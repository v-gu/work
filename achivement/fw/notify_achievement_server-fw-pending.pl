#!/usr/bin/perl -I/apps/lib

# Implementation of reading live log files inserting into MySQL
# Written by Kenny Nguyen
# 2008/08/23
#####################################
# copy this script into another name#
# cp thisFile.pl to thisFile2.pl    #
# perl thisFile2.pl                 #
# Log file is at:                   #
# /data/log/thisFile2.pl.log         #
# PID:  /data/log/run/thisFile2.pl.pid   #
#####################################

use POSIX qw(setsid);
use Date::Manip;
use Log::Dispatch;
use Log::Dispatch::FileRotate;
use File::Copy;
use Proc::PID::File;
use IO::Socket;
use Time::HiRes qw(usleep gettimeofday);
use Time::Local;
use Digest::MD5 qw(md5_hex);
use WWW::Curl::Easy;
use WWW::Curl::Form;
use serialize;
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


my $PP = $0; (my $scriptPath, $PP) = ($PP =~ /(.*\/)(.+\.pl)/);
$scriptPath		=~ s/\/$//;

my $Config		= Config::Tiny->new();
$Config			= Config::Tiny->read( $scriptPath. '/config.ini' );
my $serverlist	= $Config->{settings}->{serverlist};
my $game		= $Config->{settings}->{game};
my $lastRunPath	= $Config->{settings}->{lastRunPath};
my $fileRunPath	= $Config->{settings}->{fileRunPath};

# choose which log type
my $logtype		= $Config->{logfile}->{formatlog};

### Cassandra
my $dbport		= $Config->{cassandra}->{dbport};
my $dbhost		= $Config->{cassandra}->{dbhost};
my $keyspace	= $Config->{cassandra}->{keyspace};

### Offset time
my %offsettime;
$serverlist =~ s/\s//g;
my @slist = split (",", $serverlist);
foreach my $server(@slist)
{
	$offsettime{$server}=$Config->{offsettime}->{$server};
}
#################


# store pointer to last read position on a log file
my $lastRunFile = "$lastRunPath/$game/app.last_run.$PP";

my $pendingLog = "/apps/pending-$game.log";
my $tempLog = "/apps/pending-$game-processing.log";


# If already running, then exit
if (Proc::PID::File->running("$fileRunPath/$game")) {
    print "Daemon is running already...\n";
    exit(0);
}         

if (!-e $lastRunFile) {
    open(F, ">$lastRunFile");
    close F;
}

# Connect to the database. cf='complete'
my $cass = new CassandraPW4( $dbhost, $dbport, $keyspace );


chdir '/'                 or die "Can't chdir to /: $!";
umask 0;
open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
open STDOUT, '>/dev/null' or die "Can't write to /dev/null: $!";
open STDERR, '>/dev/null' or die "Can't write to /dev/null: $!";
defined(my $pid = fork)   or die "Can't fork: $!";
exit if $pid;
setsid                    or die "Can't start a new session: $!";

our $PROGRAM = $0; $PROGRAM =~ s|.*/||;
my $PID = $$;
print "Daemon started - processid: $PID\n";

open F, "> $fileRunPath/$game/$PROGRAM.pid" or die "Can't open pid $!";
print F $PID;
close F;


my $log = Log::Dispatch::FileRotate->new( name   => 'file1',
                                       min_level => 'info',
                                       filename  => "$lastRunPath/$game/$PROGRAM.log",
                                       mode      => 'append' ,
                                       TZ        => 'PST',
                                       DatePattern => 'yyyy-dd-HH'
                                      );

$log->log( level => 'info', message => Util::get_epoch2date(time) . " - Daemon started\n" );

my %logach = ();
my %hrole2users = ();

my $good = 0;
my $bad = 0;
sub check_pending() {
	my $good = 0;
	my $bad = 0;
	my $logDate;
	if ((-e $pendingLog) or (-e $tempLog)) {
		if(-e $tempLog) {
			$logDate = Util::get_epoch2date(time);
			$log->log( level => 'info', message => "$logDate Old Pending log exists - attempting to clear it first\n" );
			print "Old Pending log exists - attempting to clear it first\n";
			# old log exists, something bad happened. do this one first.
			# anything we can't do will go back into pending log. 
			# if we crash, old log will still exist, and we may duplicate some completions next run.
			open(TEMP, $tempLog);
			while(<TEMP>) {
				$log->log( level => 'info', message => "  " ); # read
				read_line($_);
			}
			close(TEMP);
			unlink($tempLog);
		}
		move($pendingLog,$tempLog);
		$logDate = Util::get_epoch2date(time);
		$log->log( level => 'info', message => "$logDate Pending log exists - attempting to write entries\n" );
		print "Pending log exists - attempting to write entries\n";
		open(TEMP, $tempLog);
		while(<TEMP>) {
			$log->log( level => 'info', message => "  " ); # read
			read_line($_);
		}
		close(TEMP);
		unlink($tempLog);
		$logDate = Util::get_epoch2date(time);
		$log->log( level => 'info', message => "$logDate Finished importing old entries. Starting sleeping 30.  Completed $good, could not complete $bad.\n" );
		print "$logDate Finished importing old entries. Starting sleeping 30.  Completed $good, could not complete $bad.\n";
	} else {
		$logDate = Util::get_epoch2date(time); 
		$log->log( level => 'info', message => "$logDate No pending log exists for $game\n" ); 
	}
}

while(1) {
	check_pending();
	sleep(30);
}

# Lib functions
#
sub read_line {
	my $str = shift;
    my @data = split(/,/,trim($str));
	if($#data!=3) {
		print "Got bad data from \"".trim($str)."\", ignoring line\n";		
	} elsif(complete_achievement($data[0],$data[1],$data[2],$data[3]) == 0) {
		open (PENDING, ">>$pendingLog");
        print PENDING join(',',@data)."\n";
        close PENDING;
		$bad++;
	}
}


sub get_userid {
	my $server = shift;
	my $roleid = shift;
	my $date = shift;
	my $achid = shift;

	my $rolenuser = "$server-$roleid-$game";
	my $userid;

    my %data = $cass->get('roleinfo',$rolenuser);
	$userid = $data{$rolenuser}{'userid'};
	if ($userid) {
		$log->log( level => 'info', message => "\033[0;32mc\033[m" ); # cassandra  (green c)
		$hrole2users{$server}{$roleid}=$userid;
		return $userid;
	}
	return;
}

sub complete_achievement {
	my $achid = shift;
    my $server = shift;
	my $roleid = shift;
    my $date = shift;

	$log->log( level => 'info', message => "b" ); # begin, prepare
	my $userid;
	if ($roleid =~ m/u(\d+)/) {	
		$userid = $1;
		$roleid = '';
		$log->log( level => 'info', message => "a" ); # already have userid
	} else {
		if ($hrole2users{$server}{$roleid})	{	$userid = $hrole2users{$server}{$roleid};	}
		else 								{	$userid = get_userid($server,$roleid,$date,$achid);		}
	}

	if(!$userid || $userid eq "") {
		my $logDate = Util::get_epoch2date(time);
		$log->log( level => 'info', message => "     $logDate $date Cannot find userid for $server-$roleid-$game\n" ); 
		print "     $logDate $date Cannot find userid for $server-$roleid-$game\n";
		return 0;
	}

	my ($key1, $key2, $str, $achcomplete, $msg);
    my %cdata = ();
	my %udata = ();

	my $sendSNS = 0;

    my %data = $cass->get('completed',$userid);
	$log->log( level => 'info', message => "u" ); # got userid
	
	my $logDate = Util::get_epoch2date(time);
	if(%data and $data{$userid} and $data{$userid}{$achid}) {
		$str = $data{$userid}{$achid};
	    $msg = "Userid:$userid, roleid:$roleid, server:$server and $str achivement $achid already have been completed";
	} else {
		# insert into achivement.completed[userid][achid]=roleid,server,game,date
		$achcomplete = "$roleid,$server,$game,$date";
		%cdata = ( $achid => $achcomplete );
		my $time = $cass->get_timestamp($date);
		$cass->set('completed',$userid,\%cdata,$time);

		#insert into achivement.completed2users[achid][userid]=roledid,server,game,date
		%udata = ( $userid => $achcomplete );
		$cass->set('completed2users',$achid,\%udata,$time);
		$msg = "set(achivement.completed and completed2users: $userid, $achid=>$achcomplete)";

		# get num-completed from ach data
		my %adata;
		%adata = $cass->get('data',$achid);
		my $cnt = $adata{$achid}{"completed-$server"};
		if(!$cnt) {
			$cnt = 0;
		}
		%adata = ( "completed-$server" => $cnt+1 );
		$cass->set('data',$achid,\%adata);
		$msg .= ", set(achievement.data[$achid][completed-$server]=>".$adata{"completed-$server"}.")";

		$sendSNS = 1;

		print "$logDate $date $msg\n";
	}
	$log->log( level => 'info', message => "s" ); # saved data
	if(!$logach{$achid}) {
		$logach{$achid} = Log::Dispatch::FileRotate->new( name   => 'file1',
                                       min_level => 'info',
                                       filename  => "$lastRunPath/$game/notify/$PROGRAM-$achid.log",
                                       mode      => 'append' ,
                                       TZ        => 'PST',
                                       DatePattern => 'yyyy-dd-HH'
                                      );
	}

	$log->log( level => 'info', message => "l " ); # writing log
	$log->log( level => 'info', message => "$logDate $date $msg\n" );
	$logach{$achid}->log( level => 'info', message => "$logDate $date $msg\n" );

	my $scanTime = time;
	my $logTime = Util::get_date2epoch($date) + $offsettime{$server}*3600;
	if($sendSNS and abs($scanTime-$logTime)<3600) { 
		# send message to SNS server, only if gametime is +/- 10 mins from this server's time.
		# don't want to send OLD notifications
		# function checks if sending is actually necessary from achievement data.
		my $snsdate = Util::get_epoch2date($logTime);
		sendSNS($achid,$snsdate,$userid,$roleid,$server);
	}

	my $propogate = check_dependancy($achid,$userid,$roleid,$server,$date);

	$good++;
	return $propogate;
}

# send feed to SNS api
# roleid, server, and game are used to look up rolename if necessary
sub sendSNS {
	my ($achid, $date, $userid, $roleid, $server) = @_;

	my %data = $cass->get('data',$achid);
	my $adata = $data{$achid};
	my %achdata = %$adata;
	if($achdata{'feed_title'}) {
		my $stime = Util::get_date2epoch($date,"%s");
		# $userid from above
		my $code = '$Y*n6#Pb81Kg@C!';
		my $rand = int(rand(10000))+1;
		my $verify = substr(md5_hex($code.$userid.$stime.$rand),28);
		my $url = "http://core.perfectworld.com/api/sendfeed?i=$userid&t=$stime&r=$rand&v=$verify";
		my %passdata = (
			'notice_id' => $achid,
			'ach_name' => $achdata{'name'},
			'ach_desc' => $achdata{'description'},
			'category' => $achdata{'category'},
			'roleid' => $roleid,
			'server' => $server,
			'game' => $game
		);
		# if feed requires [nickname] [charname] or [clan], sns will fill it in for us
		# anything else we have to pass in %passdata

		my %postdata = (
			'title' => $achdata{'feed_title'},
			'body' => $achdata{'feed_body'},
			'data' => serialize(\%passdata)
		);
		if(!$achdata{'point'} or $achdata{'point'}==0) {
			$postdata{'type'} = 'ingamenotification';
		} else {
			$postdata{'type'} = 'achievement';
		}

		my $curl = new WWW::Curl::Easy;
		my $frm = new WWW::Curl::Form;
		foreach my $key1 (keys %postdata) {
			$frm->curl_formadd($key1, $postdata{$key1});
		}
		my $result;
		open (my $buff, ">", \$result);
		$curl->setopt(CURLOPT_URL, $url);
		$curl->setopt(CURLOPT_HTTPPOST, $frm);
		$curl->setopt(CURLOPT_WRITEDATA, $buff);
		my $rcode = $curl->perform;

		print "called $url with ".serialize(\%passdata)." and ".serialize(\%postdata).", got $result\n";
		my $logDate = Util::get_epoch2date(time);
		$result =~ tr/\n/ /;
		$result =~ tr/\t/ /;
		if(!$logach{$achid}) {
			$logach{$achid} = Log::Dispatch::FileRotate->new( name   => 'file1',
										   min_level => 'info',
										   filename  => "$lastRunPath/$game/notify/$PROGRAM-$achid.log",
										   mode      => 'append' ,
										   TZ        => 'PST',
										   DatePattern => 'yyyy-dd-HH'
										  );
		}
		my $msg = "called $url, got $result";
		$logach{$achid}->log( level => 'info', message => "$logDate $date $msg\n" );
		print "$logDate $date $msg\n";
	}
}

sub check_dependancy {
	my $achid = shift;
	my $userid = shift;
	my $roleid = shift;
	my $server = shift;
	my $date = shift;

	my %data = $cass->get('data',$achid);
	my $parentid = $data{$achid}{'parent_id'};
#	print Dumper %data;
	if (!$parentid) {
		return 1;
	} else {
		my %pdata = $cass->get('data',$parentid);
		my $childrenid = $pdata{$parentid}{'children_id'};
		my @children = split(',',$childrenid);
		my %udata = $cass->get('completed',$userid);
		foreach $childrenid(@children)
		{
			if (!$udata{$userid}{$childrenid})
			{
				my $msg2 = "Not meeting requirement at $childrenid : userid=$userid, parentid=$parentid, children=@children";
				my $logDate2 = Util::get_epoch2date(time);
				$log->log( level => 'info', message => "        $logDate2 $date $msg2\n" );
				return 2;
			}
		}
		# complete check
		my $logDate = Util::get_epoch2date(time);
		my $msg = "Dependancy Established = orgid:$achid -- parentid:$parentid -- $server,$roleid,$date";
		$log->log( level => 'info', message => "        $logDate $date $msg\n" );		
		my $compl_achid = complete_achievement($parentid,$server,$roleid,$date);
		return $compl_achid;
	}
}

sub cleanNum {
    my $string = shift;
    $string =~ s/[^0-9]//gi;
    return $string;
}
sub trim {
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}
