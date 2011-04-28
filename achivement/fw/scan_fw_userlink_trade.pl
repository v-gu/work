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
my $logtype		= $Config->{logfile}->{formatlog};

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

## END OF DO-NOT-TOUCH #####

sub process_data {
	my $lines_ref = shift;
	foreach ( @{$lines_ref} ) {
		chomp;
		my $str = $_;
		if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) backup gdeliveryd: notice : formatlog:trade:roleidA=(\d+):roleidB=(\d+):moneyA=(\d+):moneyB=(\d+):objectsA=([^:]*):objectsB=(.*)/) {
			my ($datetime, $src_role, $dst_role, $src_money, $dst_money, $src_obj, $dst_obj) = ($1, $2, $3, $4, $5, $6, $7);
			UserlinkTrade ($server, $datetime, $src_role, $dst_role, $src_money, $dst_money, $src_obj, $dst_obj);
		}
	}
}


# Lib functions

sub userlinkSerial {
	my ($iserver, $rsg, $asg, $datetime, $status)=@_;
	my ($msg, $userid, $rsgserial, $asgserial, $unserials, $unseriala);
	my %data = ();
	
	my %datarsg = $cass->get('userlinks',$rsg);
	print "$rsg,$asg\n";
	print Dumper \%datarsg;
	$rsgserial = $datarsg{$rsg}{$asg};
	
	my $srsgserial=Util::getserial($status, $datetime, $rsgserial);

	%data = ( $asg => $srsgserial, );
	print Dumper \%data;
	my $time = $cass->get_timestamp($datetime);
	$cass->set('userlinks',$rsg,\%data);
	$msg = "set(userlinks, $rsg - $status, $asg, $datetime)";
	my $logDate = Util::get_epoch2date(time);
	$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
}


sub UserlinkTrade {
	my ($iserver, $datetime, $src_role, $dst_role, $src_money, $dst_money, $src_obj, $dst_obj)=@_;
	my $igame=$game;
	my $rsg="$iserver-$src_role-$igame";
	my $asg="$iserver-$dst_role-$igame";
	print "source money === $src_money\ndest money ==== $dst_money\n";
	print "($iserver, $datetime, $src_role, $dst_role, $src_money, $dst_money, $src_obj, $dst_obj\n";
	
	## NO MQUORUMY INVOLVE 
	if (($src_money eq '0') && ($dst_money eq '0'))
	{
		# trade item , no money
		if (($src_obj ne "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-trade item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-trade item');
		}
		# no money , source has item, dest = no item ==> dest giving
		if (($src_obj ne "") && ($dst_obj eq ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-receive item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-give item');
		}
		# soucce = no item, dest = have item ==> source giving
		if (($src_obj eq "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-give item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-receive item');
		}
	}
	## SOURCE WITH MQUORUMY -- DEST NO MQUORUMY
	if (($src_money > 0) && ($dst_money == 0))
	{
		# source recieving money and item, dest receive item 
		if (($src_obj ne "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-trade money-item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-trade 0-item');
		}
		# sourse receive money and item, dest doesn't receive any thing
		if (($src_obj ne "") && ($dst_obj eq ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-receive money-item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-give money-item');
		}
		# receive money and no item
		if (($src_obj eq "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-sell item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-buy item');
		}
		# receive money only
		if (($src_obj eq "") && ($dst_obj eq ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-receive money');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-give money');
		}
	}
	## SOURCE NO MQUORUMY -- DEST WITH MQUORUMY
	if (($src_money == 0) && ($dst_money > 0))
	{
		# source no money and item, dest receive money and item
		if (($src_obj ne "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-trade 0-item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-trade money-item');
		}
		# source no money but receive item = buy, dest have money but no item
		if (($src_obj ne "") && ($dst_obj eq ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-buy item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-sell item');
		}
		if (($src_obj eq "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-give money-item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-receive money-item');
		}
		# give money away
		if (($src_obj eq "") && ($dst_obj eq ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-give money');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-receive money');
		}
	}
	## SOURCE WITH MQUORUMY -- DEST WITH MQUORUMY
	if (($src_money > 0) && ($dst_money > 0))
	{
		# exchange item and money
		if (($src_obj ne "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-trade money-N-item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-trade money-N-item');
		}
		# source item, dest no item
		if (($src_obj ne "") && ($dst_obj eq ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-money receive-item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-money give-item');
		}
		if (($src_obj eq "") && ($dst_obj ne ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-money give-item');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-money receive-item');
		}
		# exchange money -- only god know why !!!!!
		if (($src_obj eq "") && ($dst_obj eq ""))
		{
			userlinkSerial($iserver, $rsg, $asg, $datetime, 'trade-trade money');
			userlinkSerial($iserver, $asg, $rsg, $datetime, 'trade-trade money');
		}	
	}
}
