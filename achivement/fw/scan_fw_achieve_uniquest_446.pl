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

# PWESSANDRA CONFIG VARIABLES
my $dbhost = "172.29.1.165";
my $dbport = "9160";
my $keyspace = "game_6";

####################################
#   do not touch this              #
####################################

# Connect to the database.
my $cass = new CassandraPW4( $dbhost, $dbport, $keyspace );


my %huniquests	=	(
						446	=>	10,
					);


###	TASK START ###

my @achievementIds;
re_assignhash(\%huniquests);

sub re_assignhash{
	my ($tmp) = @_;
	my %htmp = %$tmp;
	foreach my $key (keys %htmp)
	{   push (@achievementIds, $key);   }
}


# get achievement details (min/max value)
my %achievements = Util::get_achievement_data($server,\@achievementIds);
my $zone = Util::get_server_zone($server);
my $cass2 = Util::connect_cass("cluster_$server",undef,undef,"progress_$zone");
my (%hrole2task, %hcompleted, %hrole2ach); 
my $cred="\e[1;31m";
my $ccya="\e[1;32m";
my $cyel="\e[1;33m";
my $cnor="\e[0m";

my $logPath = "/data2/logs/$game/";
our $PROGRAM = $0; $PROGRAM =~ s|.*/||;

my $logDateD = Util::get_epoch2date(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";

get_complete2users (\%huniquests);

sub get_complete2users {
	my ($tmpvalue) = @_;
	my %htmphash = %$tmpvalue;
	foreach my $ach (keys %htmphash)
	{
		my %usercomplete = $cass->get('completed2users', $ach); 
		foreach my $userid (keys %{$usercomplete{$ach}})
		{
			$hcompleted{$userid}{$ach} = 1;
		}
	}
}

my $tlogDateD = Util::get_epoch2date(time);
print "$logDateD $tlogDateD : complete read from DB for 'completed2users'\n";
sleep 5;

# before tail, scan all of today's log
open(FILE, "<$file") or die "Can't open $file\n";
while(<FILE>) {
	chomp;
	process_data([$_]);
}


print "START: $logDateD --- SERVER: $server --- FILE: $file\n";
$logDateD = Util::get_epoch2date(time);
print "END: $logDateD --- SERVER: $server --- FILE: $file\n";

#   1   <=>     value   =>  date
#   2   <=>     date    =>  value
#   3   <=>     value compare directly

sub process_data {
    my $lines_ref = shift;
    foreach ( @{$lines_ref} ) {
        chomp;
		my $str = $_;

		# unique quest
		if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+gamed: notice : formatlog:task:roleid=(\d+):taskid=(\d+):type=.+:msg=finishtask,level=\d+,success=1,giveup=/) {
 			my ($datetime, $roleid, $taskid) = ($1, $2, $3);
			$hrole2task{$roleid}{$taskid} = $datetime if (!$hrole2task{$roleid}{$taskid});
			achievementcheck ($server, $datetime, $roleid, $taskid, \%huniquests, 1);

		# get $roleid to userid login
		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) backup glinkd-\d+: notice : formatlog:rolelogin:userid=(\d+):roleid=(\d+)/) {
			my ($datetime, $userid, $roleid) = ($1, $2, $3);
			foreach my $ach (keys %{$hcompleted{$userid}})
			{
				$hrole2ach{$roleid}{$ach} = 1 if ($hcompleted{$userid}{$ach});
			}
		}
    }
}
#   1   <=>     value   =>  date
#   2   <=>     date    =>  value
#   3   <=>     value compare directly

# Lib functions
# return number of quest completed

sub achievementcheck {
    my ($server, $datetime, $roleid, $value, $tmphash, $stat)=@_;
	return if ($datetime !~ /....-..-.. ..:..:../);
	my %hhash = %$tmphash;
	my $logDate = Util::get_epoch2date(time);
	foreach my $ach (keys %hhash)
	{
		next if (!$hhash{$ach});
		if ($hrole2ach{$roleid}{$ach})
		{
			print "$logDateD $logDate $datetime roleid:$roleid , achid:$ach completed.  $ccya SKIP DB : skip loop $cnor\n"; 
			next;
		}
		my $max = $achievements{$ach}{'award_value'};
		if (!$hrole2task{$roleid}{$ach}{$value})
		{
			updateProgress($ach, $server, $roleid, $datetime, $value, $datetime) if ($stat eq 1);
			updateProgress($ach, $server, $roleid, $datetime, $datetime, $value) if ($stat eq 2);
		}
		my $have;
		$have = getNumCompleted_Task($ach,$server,$roleid,$datetime,$value) if (($stat eq 1) or ($stat eq 2));
		$have = $value if ($stat eq 3);
		next if ((!$have) or (!$max));
		if($have>=$max and $have>0 and $have<100000000000 and $max>0 and $max<100000000000) {
			my $msg = Util::completeAchievement($ach,$server,$roleid,$datetime);
			my $deltime = $cass2->get_timestamp($datetime);
			$cass2->del('progress', "$server-$roleid-$ach", ($deltime+1)) if (($msg) and ($msg !~ /already completed .+skipping/) and ($stat ne 3));
			print "$logDateD $msg";
			$hrole2ach{$roleid}{$ach} = 1;
		}
	}
}

sub getNumCompleted_Task {
	my ($id, $server, $roleid, $datetime, $taskid) = @_;

	my $msg = "count(progress, $server-$roleid-$id)";
	my $count = 0;
	if (($hrole2task{$roleid}{$id}{$taskid}) and ($hrole2task{$roleid}{$id}{'valid'}))
	{
		$hrole2task{$roleid}{$id}{$taskid} = 1;
		foreach my $tmptaskid (keys %{$hrole2task{$roleid}{$id}})
		{
			next if ($tmptaskid eq 'valid');
			$count++ if ($hrole2task{$roleid}{$id}{$tmptaskid});
		}
		$msg .= " :$cred SKIP DB$cnor";
	} else {
		my %tmphash = $cass2->get('progress',"$server-$roleid-$id");
		foreach my $tmptaskid (keys %{$tmphash{"$server-$roleid-$id"}})
		{
			next if (!$tmphash{"$server-$roleid-$id"}{$tmptaskid});
			$count++;
			$hrole2task{$roleid}{$id}{$tmptaskid} = 1;
		}
		$hrole2task{$roleid}{$id}{$taskid} = 1;
		$hrole2task{$roleid}{$id}{'valid'} = 1;
		$msg .= " :$cyel GET DB$cnor";
	}

	my $logDate = Util::get_epoch2date(time);
	print "$logDateD $logDate $datetime $msg   => $count\n";

	return $count;
}

sub updateProgress {
	my ($ach,$server,$roleid,$datetime,$column,$value)=@_;
	return if ($datetime !~ /....-..-.. ..:..:../);

    my %data = ( $column => $value );

	my $time = $cass2->get_timestamp($datetime);
	$cass2->set('progress',"$server-$roleid-$ach",\%data,$time);

	my $msg = "set(progress, $server-$roleid-$ach, $column => $value, $time)";
    my $logDate = Util::get_epoch2date(time);
	print "$logDateD $logDate $datetime $msg\n";
}
