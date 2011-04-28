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
use serialize;
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
my $logtype		= $Config->{logfile}->{trace};

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

#chdir '/'                 or die "Can't chdir to /: $!";
#umask 0;
#open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
#open STDOUT, '>/dev/null' or die "Can't write to /dev/null: $!";
#open STDERR, '>/dev/null' or die "Can't write to /dev/null: $!";
#defined(my $pid = fork)   or die "Can't fork: $!";
#exit if $pid;
#setsid                    or die "Can't start a new session: $!";

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


my $cass = new CassandraPW_Time( $dbhost, $dbport, $keyspace );
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
		if ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) backup unamed: debug : unamed::ReleaseRoleName: zoneid=(.+),userid=(.+),roleid=(.+),rolename=\"(.+)\"/) {
			my ($datetime, $zoneid, $userid, $roleid, $rolename) = ($1, $2, $3, $4, $5);
			my $rolenuser="$zoneid-$roleid-$game";
			my $time = $cass->get_timestamp($datetime);
			print "From start: $datetime, $roleid, $zoneid, $rolenuser, $time\n";
			insert2DB ($datetime, $cass, 'roleinfo', "$zoneid-$roleid-$game-stat", $datetime, "deleted,$userid,$rolename");
			delProgress($datetime, $roleid, $zoneid, $rolenuser, $userid, $time);
			delUser2roles($datetime, $roleid, $zoneid, $rolenuser, $userid, $time);
			delRoleinfo($datetime, $roleid, $zoneid, $rolenuser, $userid, $time);
			delUserlinks($datetime, $roleid, $zoneid, $rolenuser, $userid, $time);
		}
	}
}

sub insert2DB {
	my ($datetime, $cassvar, $colfamily, $key, $colname, $colval) = @_;
	my %hhash = ($colname	=>	$colval 	);
	my $time = $cassvar->get_timestamp($datetime);
	my $logDate = Util::get_epoch2date(time);
	$cassvar->set($colfamily, $key, \%hhash, $time);
	my $msg = "set($colfamily, $key, $colname => $colval)";
	$log->log( level => 'info', message => "$logDate $datetime $msg\n");
	print "$logDate $datetime $msg\n";
}

sub get_userid {
	my $server = shift;
	my $roleid = shift;
	my $date = shift;
	my $rolenuser = "$server-$roleid-$game";
	my $userid;
	my %data = $cass->get('roleinfo',$rolenuser);
	$userid=$data{$rolenuser}{'userid'}{'value'};
	if ($userid){
		return $userid;
	}
	return -1;
}

sub delUserlinksExec {
	my ($time, $unvalue, $datetime, $deltime, $rsg, $asg) = @_;
	my $unserial = unserialize($unvalue);
	my %hserial = %$unserial;

	my $logDate = Util::get_epoch2date(time);

	foreach my $hkey(keys %hserial)
	{
		my $ctime = $cass->get_timestamp($hserial{$hkey}); 
		if ($ctime <= $deltime)
		{
			print "Delete a key : $rsg : $asg : $hkey => $hserial{$hkey} :::  $ctime <= $deltime\n";
			delete $hserial{$hkey};
		}
	}

	if (%hserial)
	{	
		my $iserial = serialize(\%hserial);
		my %idata = ( $asg 	=>	$iserial );
		$cass->set('userlinks',$rsg, \%idata);
		my $msg = "Reset from userlink - $rsg : $asg : $time";
		$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
		print "$logDate $datetime -- $msg\n";
	} else
	{	
		$cass->del('userlinks',$rsg,$time,$asg);
		my $msg = "Del from userlink - $rsg : $asg : $time";
		$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
		print "$logDate $datetime -- $msg\n";
	}
}


sub delUserlinks {
	my ($datetime, $roleid, $iserver, $rolenuser, $userid, $time)=@_;
	print "userlinks: $datetime, $roleid, $iserver, $rolenuser, $userid, $time\n";
	my %data = $cass->get('userlinks',$rolenuser);
	return if (!($data{$rolenuser}));

	my $msg;
	my $logDate = Util::get_epoch2date(time);
	my $deltime = $cass->get_timestamp($datetime);
	foreach my $key(keys %{$data{$rolenuser}})
	{
		if ($key =~ /\d+-\d+-\w+/)
		{
			my $inserttime = $data{$rolenuser}{$key}{'timestamp'} + 1;
			delUserlinksExec ($inserttime, $data{$rolenuser}{$key}{'value'}, $datetime, $deltime, $rolenuser, $key );

			my %datakey = $cass->get('userlinks',$key);
			next if (!($datakey{$key}{$rolenuser}));
			$inserttime = $datakey{$key}{$rolenuser}{'timestamp'} + 1;
			delUserlinksExec ($inserttime, $datakey{$key}{$rolenuser}{'value'}, $datetime, $deltime, $key, $rolenuser);
		}	
	}
}

sub delProgress {
	my ($datetime, $roleid, $iserver, $rolenuser, $userid, $time)=@_;
	my $logDate = Util::get_epoch2date(time);
	my $msg;
	my %hachievementindex = $cass->get('index', 'achievementid');
	my @achievearray = keys %{$hachievementindex{'achievementid'}};
	my $cass2 = Util::connect_cass("cluster_$iserver",undef,undef,"progress_".Util::get_server_zone($iserver));
	foreach my $i (@achievearray)
	{
		$cass2->del('progress',"$iserver-$roleid-$i",$time);
		$msg = "Del from progress = server:$iserver role:$roleid  -- key: $i";
		$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
		print "$logDate $datetime $msg  --  delProgress\n";
	}
}

sub delUser2roles {
	my ($datetime, $roleid, $iserver, $rolenuser, $userid, $time)=@_;
	my $logDate = Util::get_epoch2date(time);
	print "delUser2roles - $datetime, $roleid, $iserver, $rolenuser, $userid, $time\n";
	my %data = (	$rolenuser	=>	'',
				);
	$cass->set('user2roles',$userid,\%data,$time) if ($userid);
	my $msg = "Update user2roles : $userid = $rolenuser => ''";
	$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
	print "$logDate $datetime $msg  --  delUser2roles\n";
}

sub delRoleinfo {
	my ($datetime, $roleid, $iserver, $rolenuser, $userid, $time)=@_;
	my $logDate = Util::get_epoch2date(time);
	my %data = (
		'deleted'		=> $datetime,
		'created'		=> '',
		'occupation'	=> '',
		'gender'		=> '',
		'face'			=> '',
		'forumid'		=> '',
		'lastlogin'		=> '',
		'lastlogout'	=> '',
		'isActive'		=> '',
		'level'			=> '',
		'total_time'	=> '',
		'pvpkills'		=> '',
		'killsUpdate'	=> '',
		'guild'			=> '',
		'name'			=> '',
		'money'			=> '',
		'lastguildjoin' => '',
		
	);	
	$cass->set('roleinfo',$rolenuser,\%data,$time);
	my $msg = "Update roleinfo : $rolenuser => ALL empty string";
	$log->log( level => 'info', message => "$logDate $datetime $msg\n" );
	print "$logDate $datetime $msg  --  delRoleinfo\n";
}
