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
my $PP = $0; (my $scriptPath, $PP) = ($PP =~ /(.*\/)(.+\.pl)/);
$scriptPath		=~ s/\/$//;

my $Config		= Config::Tiny->new();
$Config			= Config::Tiny->read( $scriptPath. '/config.ini' );
my $server		= $Config->{settings}->{server};
my $game		= $Config->{settings}->{game};
my $lastRunPath	= $Config->{settings}->{lastRunPath};
my $fileRunPath	= $Config->{settings}->{fileRunPath};

# choose which log type
my $logtype		= $Config->{logfile}->{wlog};

### Cassandra
my $dbport		= $Config->{cassandra}->{dbport};
my $dbhost		= $Config->{cassandra}->{dbhost};
my $keyspace	= $Config->{cassandra}->{keyspace};


# store pointer to last read position on a log file
my $lastRunFile = "$lastRunPath/$game/$server/app.last_run.$PP";

my @logFile;
my $count = 0;
$logFile[$count] = $logtype;

####################################
#   do not touch this              #
####################################

# If already running, then exit
if (Proc::PID::File->running("$fileRunPath/$game/$server")) {
	print "Daemon is running already...\n";
	exit(0);
} else {	print "Daemon started - $0\n"	};

if (!-e $lastRunFile) {
	open(F, ">$lastRunFile");
	close F;
}

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


open F, "> $fileRunPath/$game/$server/$PROGRAM.pid" or die "Can't open pid $!";
print F $PID;
close F;


my $log = Log::Dispatch::FileRotate->new(
											name => 'file1',
											min_level => 'info',
											filename => "$lastRunPath/$game/$server/$PROGRAM.log",
											mode => 'append' ,
											TZ => 'PST',
											DatePattern => 'yyyy-dd-HH'
);

$log->log( level => 'info', message => Util::get_epoch2date(time) . " - Daemon started\n" );

# Connect to the database.

my $cass = new CassandraPW( $dbhost, $dbport, $keyspace );
$cass->set_consistency( # set consistency levels. default for read/write is QUORUM
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM,	# read
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM	# write
);

# initiate main function for the log file

my $tail = File::Tail::Multi->new (
	OutputPrefix => "p",
	Debug => 0,
	NumLines => 1000,
	LastRun_File => "$lastRunFile",
	Function => \&process_data,
	Files => [@logFile],
	RemoveDuplicate => 1
);

my $lastupdate = time;
while(1) {
	$tail->read;
	usleep(200000);
}

## END OF DO-NOT-TOUCH #####

sub process_data {
	my $lines_ref = shift;
	foreach ( @{$lines_ref} ) {
		chomp;
		my $str = $_;
		if ($str =~ /(\d+-\d+-\d+ \d+:\d+:\d+) .+gamed: info : player_kill,killer=(.+),killer_level=(.+),killer_pkval=(.+),victim=(.+),victim_level=(.+),victim_pkval=(.+)/) {
			my ($datetime, $attackerid, $attackerlevel, $attackerpk, $roleid, $rolelevel, $rolepk) = ($1, $2, $3, $4, $5, $6, $7);
			my $levefdiff = $rolelevel - $attackerlevel;
			UserlinkPVP($server, $datetime, $roleid, $attackerid, $game) if (($attackerid ne $roleid) and (($levefdiff >= -30) and ($levefdiff <= 30)));
		}
	}
}


# Lib functions

sub userlinkSerial {
	my ($iserver, $rsg, $asg, $datetime, $status, $roleid, $victimid)=@_;
	my ($pvpkill, $dateold, $msg, $userid, $rsgserial, $asgserial, $unserials, $unseriala);
	my %data = ();
	my %metadata = ();
	my %datarsg = ();
	my %datauser = ();
	
	%datarsg = $cass->get('userlinks',$rsg);
	$rsgserial = $datarsg{$rsg}{$asg};
	
	my $srsgserial=Util::getserial($status, $datetime, $rsgserial);

	%data = ( $asg => $srsgserial, );
	my $time = $cass->get_timestamp($datetime);
	$cass->set('userlinks',$rsg,\%data);
	$msg = "set(userlinks, $rsg - $status, $asg, $datetime)";
	my $logDate = Util::get_epoch2date(time);
	$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
	if ($status eq "kill")
	{	
		%data = ();

		%data = ( 'killsUpdate' => $datetime, );
		$cass->set('roleinfo',$rsg,\%data,$time);
		$msg = "set(roleinfo, $rsg - 'killsUpdate'   => $datetime, $asg, $datetime)";
		$logDate = Util::get_epoch2date(time);
		$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
		print "$logDate $datetime $msg\n";	
	}
}

sub UserlinkPVP {
	my ($iserver, $datetime, $roleid, $attacker, $igame)=@_;
	my $rsg="$iserver-$roleid-$igame";
	my $asg="$iserver-$attacker-$igame";
	#get to the victim -> killer
	userlinkSerial($iserver, $rsg, $asg, $datetime, 'killed');
	userlinkSerial($iserver, $asg, $rsg, $datetime, 'kill', $attacker, $roleid);
}
