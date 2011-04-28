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

# Connect to the database.
my $cass = new CassandraPW4( $dbhost, $dbport, $keyspace );

my %hpvpkills   =   (
						442 =>  1,          # kill 1
					);

my %hmoney      =   (
						432 =>  1000000,    # 1 coin = 1 mil money
						433 =>  5000000,    # 1 coin = 1 mil money
					);

my %hpetget     =   (
						437 =>  1,          # one
						438 =>  5,          # five total
					);
my %hpetrename  =   (
						439 =>  1,
					);

my @achievementIds;
re_assignhash(\%hpvpkills);
re_assignhash(\%hmoney);
re_assignhash(\%hpetget);
re_assignhash(\%hpetrename);

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
my %hroleid2occ; 

my $logPath = "/data2/logs/$game/";
our $PROGRAM = $0; $PROGRAM =~ s|.*/||;

my $logDateD = Util::get_epoch2date(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";
`echo "START: $logDateD --- SERVER: $server --- FILE: $file" >> $logPath/$PROGRAM-server-$server`;
# before tail, scan all of today's log
open(FILE, "gunzip -c $file |") or die "Can't open $file\n";
while(<FILE>) {
	chomp;
	process_data([$_]);
}

$logDateD = Util::get_epoch2date(time);
print "END: $logDateD --- SERVER: $server --- FILE: $file\n";
`echo "END: $logDateD --- SERVER: $server --- FILE: $file" >> $logPath/$PROGRAM-server-$server`;

#   1   <=>     value   =>  date
#   2   <=>     date    =>  value
#   3   <=>     value compare directly


sub process_data {
    my $lines_ref = shift;
    foreach ( @{$lines_ref} ) {
        chomp;
		my $str = $_;

		# pvp within 5 level
		if ($str =~ /(\d+-\d+-\d+ \d+:\d+:\d+) .+gamed: info : player_kill,killer=(.+),killer_level=(.+),killer_pkval=(.+),victim=(.+),victim_level=(.+),victim_pkval=(.+)/) {
			my ($datetime, $attackerid, $attackerlevel, $attackerpk, $roleid, $rolelevel, $rolepk) = ($1, $2, $3, $4, $5, $6, $7);
			my $levefdiff = $rolelevel - $attackerlevel;
			achievementcheck ($server, $datetime, $attackerid, $roleid, \%hpvpkills, 3) if (($attackerid ne $roleid) and (($levefdiff >= -5) and ($levefdiff <= 5)));

		# money
		} elsif ($str =~ /(\d+-\d+-\d+ \d+:\d+:\d+) .+gamed: info : stat:userid=(\d+):roleid=(\d+):offline=.+:money=(\d+):cashused=/) {
			my ($datetime, $userid, $roleid, $money) = ($1, $2, $3, $4);
			achievementcheck ($server, $datetime, $roleid, $money, \%hmoney, 3);

		# pet : get 
		} elsif ($str =~ /(\d+-\d+-\d+ \d+:\d+:\d+) .+gamed: info : pethatch,roleid=(\d+),level=.+,petid=(\d+),petgid=/) {
			my ($datetime, $roleid, $petid) = ($1, $2, $3);
			achievementcheck ($server, $datetime, $roleid, $petid, \%hpetget, 1);

		# pet : rename
		} elsif ($str =~ /(\d+-\d+-\d+ \d+:\d+:\d+) .+gamed: info : petrename,roleid=(\d+),level=.+,petid=(\d+),petgid=/) {
			my ($datetime, $roleid, $petid) = ($1, $2, $3);
			achievementcheck ($server, $datetime, $roleid, $petid, \%hpetrename, 3);

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
	my %hhash = %$tmphash;
	foreach my $ach (keys %hhash)
	{
		next if (!$hhash{$ach});
		my $max = $achievements{$ach}{'award_value'};
		updateProgress($ach, $server, $roleid, $datetime, $value, $datetime) if ($stat eq 1);
		updateProgress($ach, $server, $roleid, $datetime, $datetime, $value) if ($stat eq 2);
		my $have;
		$have = getNumCompleted($ach,$server,$roleid,$datetime) if (($stat eq 1) or ($stat eq 2));
		$have = $value if ($stat eq 3);
		next if ((!$have) or (!$max));
		if($have>=$max and $have>0 and $have<100000000000 and $max>0 and $max<100000000000) {
			my $msg = Util::completeAchievement($ach,$server,$roleid,$datetime);
#my $msg = "achievement completed : aid: $ach, ==  $server, $datetime, $roleid, $value\n";
			my $deltime = $cass2->get_timestamp($datetime);
			$cass2->del('progress', "$server-$roleid-$ach", ($deltime+1)) if (($msg) and ($stat ne 3));
			print $msg;
		}
	}
}

sub getNumCompleted {
	my ($id, $server, $roleid, $datetime) = @_;

	my $count = $cass2->count('progress',"$server-$roleid-$id");
	my $msg = "count(progress, $server-$roleid-$id)";

	my $logDate = Util::get_epoch2date(time);
	print "$logDate $datetime $msg   => $count\n";

	return $count;
}

sub updateProgress {
	my ($ach,$server,$roleid,$datetime,$column,$value)=@_;

    my %data = ( $column => $value );

	my $time = $cass2->get_timestamp($datetime);
	$cass2->set('progress',"$server-$roleid-$ach",\%data,$time);

	my $msg = "set(progress, $server-$roleid-$ach, $column => $value, $time)";
    my $logDate = Util::get_epoch2date(time);
	print "$logDate $datetime $msg\n";
}
