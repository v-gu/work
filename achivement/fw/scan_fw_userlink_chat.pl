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
my $logtype		= $Config->{logfile}->{chat};

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
my $cass = new CassandraPW4( $dbhost, $dbport, $keyspace );
$cass->set_consistency(	# set consistency levels. default for read/write is QUORUM
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

sub process_data {
	my $lines_ref = shift;
	foreach ( @{$lines_ref} ) {
		chomp;
		my $str = $_;
		if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) backup glinkd-\d+: chat : Whisper: src=(\d+) dst=(\d+) msg=.+/) {
			my ($datetime, $roleidfrom, $roleidto) = ($1, $2, $3);
			userlinkChat($server, $datetime, $roleidfrom, $roleidto,'whisperFrom');
			userlinkChat($server, $datetime, $roleidto, $roleidfrom,'whisperReceive');
			$lastupdate = time;
		}
	}
}

# Lib functions
#
sub userlinkChat {
	my ($iserver, $datetime, $roleidfrom, $roleidto, $status)=@_;
	my $rsg="$iserver-$roleidfrom-$game";
	my $asg="$iserver-$roleidto-$game";
	my ($dateold, $msg, $userid, $rsgserial, $unserials);

	my %data = ();
	my %datarsg = ();
	%datarsg = $cass->get('userlinks',$rsg);
	$rsgserial = $datarsg{$rsg}{$asg};

	my $srsgserial = Util::getserial($status, $datetime, $rsgserial);
	my $time = $cass->get_timestamp($datetime);

	%data = ( $asg => $srsgserial, );
	$cass->set('userlinks',$rsg,\%data,$time);
	$msg = "set(userlinks, $rsg - $status, $asg, $datetime)";

	my $logDate = Util::get_epoch2date(time);
	$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
	print "$logDate $datetime $msg\n";
}
