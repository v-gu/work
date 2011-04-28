#!/usr/bin/perl -I/apps/lib
#
# Implementation of reading live log files inserting into MySQL
# Written by Greg Heckenbach
# 2010-04-30
# Modified by Don for Hotk
# ????-??-??
# Modified by Arnold Domingo for Forsaken World
# 2011-03-02
#

use POSIX qw(setsid);
use File::Tail::Multi;
use Date::Manip;
use Log::Dispatch;
use Log::Dispatch::FileRotate;
use Proc::PID::File;
use IO::Socket;
use Encode qw/decode/;
use HTML::Entities;
use MIME::Base64;
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
$cass->set_consistency(	# set consistency levels. default for read/write is QUORUM
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM,	# read
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM	# write
);

# initiate main function for the log file
#
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
		if ($str =~ /(\d+-\d+-\d+ \d+:\d+:\d+) .+gamed: info : stat:userid=(\d+):roleid=(\d+):offline=.+:money=(\d+):cashused=/) {
			my ($datetime, $userid, $roleid, $money) = ($1, $2, $3, $4);
			insertroleid ($server, $datetime, $roleid, $money);
		} elsif ($str =~ /'/) {
			$str = encode_entities(decode("gb2312", $str));
			if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) game\d+ gamed\d*: info : &#x7528;&#x6237;(\d+)&#x4ECE;&#x6570;&#x636E;&#x5E93;&#x53D6;&#x5F97;&#x6570;&#x636E;&#xFF0C;&#x804C;&#x4E1A;[^,]+,&#x7EA7;&#x522B;[^\s]+\s&#x540D;&#x5B57;&#39;([^&]+)&#39;/) {
				# regular expression modified for Forsaken World  --arnold.domingo
				my ($datetime, $roleid, $encname) = ($1, $2, $3);
				my $name = decode_base64($encname);
				$name =~ s/\0//g;
				updaterolename ($server, $datetime, $roleid, $name);
			}
		}
	}
}

# Lib functions
#

sub insertroleid {
	my ($iserver, $datetime, $roleid, $money)=@_;
	my $roleidserver="$iserver-$roleid-$game";
	my %data = ( 'money' => "$money,$datetime", );
	my $time = $cass->get_timestamp($datetime);
	$cass->set('roleinfo',$roleidserver,\%data, $time);

	my $msg = "(roleinfo, $roleidserver, money => $money,$datetime)";
	my $logDate = Util::get_epoch2date(time);
	$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
	print "$logDate $datetime $msg -- save_data \n";
}

sub updaterolename {
	my ($iserver, $datetime, $roleid, $name)=@_;
	my $roleidserver="$iserver-$roleid-$game";
	my $msg ="update name -- ";
	print "$iserver, $datetime, $roleid, name\n";
	my %data = (	'name'	=>	$name );
	my $time = $cass->get_timestamp($datetime);
	$cass->set('roleinfo',$roleidserver,\%data, $time);

	$msg = "$msg (roleinfo, $roleidserver, name => $name)";
	my $logDate = Util::get_epoch2date(time);
	$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
	print "$logDate $datetime $msg -- updaterolename \n";

	$name =~ tr/[A-Z]/[a-z]/;
	my %index = ( $roleidserver => 1 );
	$cass->set('rolename',$name,\%index,$time);
	$msg = "$msg (rolename, $name, $roleidserver => 1)";
	print "$logDate $datetime $msg -- updaterolename \n";
}
