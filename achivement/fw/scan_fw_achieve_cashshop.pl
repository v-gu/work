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


my %hlevelmarkman	=	(	
						386	=>	10,		# level 10
						387	=>	30,		# level 30
						388	=>	50,		# level 50
						389	=>	70,		# level 70
					);
my %hlevelwarrior	=	(	
						390	=>	10,		# level 10
						391	=>	30,		# level 30
						392	=>	50,		# level 50
						393	=>	70,		# level 70
					);
my %hlevelpriest	=	(	
						394	=>	10,		# level 10
						395	=>	30,		# level 30
						396	=>	50,		# level 50
						397	=>	70,		# level 70
					);
my %hlevelbard		=	(	
						398	=>	10,		# level 10
						399	=>	30,		# level 30
						400	=>	50,		# level 50
						401	=>	70,		# level 70
					);
my %hlevelassassin	=	(	
						402	=>	10,		# level 10
						403	=>	30,		# level 30
						404	=>	50,		# level 50
						405	=>	70,		# level 70
					);
my %hlevelprotector	=	(	
						406	=>	10,		# level 10
						407	=>	30,		# level 30
						408	=>	50,		# level 50
						409	=>	70,		# level 70
					);
my %hlevelmage	=	(	
						410	=>	10,		# level 10
						411	=>	30,		# level 30
						412	=>	50,		# level 50
						413	=>	70,		# level 70
					);
my %hlevelvampire	=	(	
						414	=>	10,		# level 10
						415	=>	30,		# level 30
						416	=>	50,		# level 50
						417	=>	70,		# level 70
					);

my %hlevelmisc 	=	(
						418	=>	20,		# level 20 - level 1 socialite
						421	=>	30,		# level 30 - level 1 adventurer
						423	=>	40,		# level 40 - level 1 merchant
					);

my %hgshop		=	(
						434	=>	5,		# buy 5 items
						435	=>	50,		# buy 50 items
						436	=>	500,	# buy 500 items
					);

my %hguildcreate=	(
						448	=>	1,		# create guild
					);
my %hguildjoin	=	(
						447	=>	1,		# join guild
					);
my %hguildupgrade=	(
						449	=>	2,		# level 2
						450	=>	3,		# level 3
					);

my %huniquests	=	(
						443	=>	10,
						444	=>	50,
						445	=>	250,
						446	=>	1000,
					);


### MOUNT START ###
my %hspecmount	=	(
						427	=>	1,
					);
my %hallmount	=	(
						440	=>	1,
					);
my %hmountrepNguild	= 	(
						22578	=>	1,	# rep mount
						22586	=>	1,	# rep mount
						22584	=>	1,	# rep mount
						22582	=>	1,	# rep mount
						22580	=>	1,	# rep mount
						22636	=>	1,	# rep mount
						22576	=>	1,	# guild mount	
						22577	=>	1,	# guild mount	
					);
my %hmountreg	=	(
						22574	=>	1,	# reg mount
						22575	=>	1,	# reg mount
						22579	=>	1,	# reg mount
						22587	=>	1,	# reg mount
						22585	=>	1,	# reg mount
						22583	=>	1,	# reg mount
						22581	=>	1,	# reg mount
						22637	=>	1,	# reg mount
					);
### MOUNT END ###

###	TASK START ###
my %htasks	=	(
						4010	=> 419,	# Achieve Level 1 Miner
						4008	=> 420,	# Achieve Level 1 Artisan
						9989	=> 422,	# Achieve Level 1 Tamer
						10210	=> 424,	# Achieve Level 1 Scavenger
						10213	=> 425,	# Achieve Level 1 Armscrafter - forging
						10218	=> 425,	# Achieve Level 1 Armscrafter - enchanting
						10270	=> 426,	# Achieve Level 1 Gearcrafter - heavy
						10272	=> 426,	# Achieve Level 1 Gearcrafter - light
						10274	=> 426,	# Achieve Level 1 Gearcrafter - cloth
						4009	=> 428,	# Achieve Level 1 Botanist
						4011	=> 429,	# Achieve Level 1 Armscrafter
						4000	=> 430,	# Achieve Level 1 Cook
						4006	=> 431,	# Achieve Level 1 Fisherman
						7874	=> 441,	# Catch one fish during the fishing event
				);

my %hprogressdate;
my %htaskachieve = reverse %htasks;

###	TASK START ###

my @achievementIds;
re_assignhash(\%hlevelmarkman);
re_assignhash(\%hlevelwarrior);
re_assignhash(\%hlevelpriest);
re_assignhash(\%hlevelbard);
re_assignhash(\%hlevelassassin);
re_assignhash(\%hlevelprotector);
re_assignhash(\%hlevelmage);
re_assignhash(\%hlevelvampire);
re_assignhash(\%hlevelmisc);
re_assignhash(\%hgshop);
re_assignhash(\%hguildcreate);
re_assignhash(\%hguildjoin);
re_assignhash(\%hguildupgrade);
re_assignhash(\%huniquests);
re_assignhash(\%hspecmount);
re_assignhash(\%hallmount);
re_assignhash(\%htaskachieve);

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
my (%hroleid2occ, %hgshopcount); 

my $logPath = "/data2/logs/$game/";
our $PROGRAM = $0; $PROGRAM =~ s|.*/||;

my $logDateD = Util::get_epoch2date(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";
`echo "START: $logDateD --- SERVER: $server --- FILE: $file" >> $logPath/$PROGRAM-server-$server`;
# before tail, scan all of today's log
open(FILE, "<$file") or die "Can't open $file\n";
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

		# gshop -- all mount
		if ($str =~ /(\d+-\d+-\d+ \d+:\d+:\d+) .+gamed: notice : formatlog:gshop_trade:userid=.+:roleid=(.+):order_id=.+:item_id=(.+):expire=.+:item_count=.+:cash_need=/) {
			my ($datetime, $roleid, $itemid) = ($1, $2, $3);
			my @tmpcount = keys %{$hgshopcount{$roleid}{$datetime}};
			my $shopcount = @tmpcount;
			my $gvalue=1;
			if (!$shopcount)
			{
				$hgshopcount{$roleid}{$datetime}{1}=1;
			} else {
				$gvalue = $shopcount + $gvalue;	
				$hgshopcount{$roleid}{$datetime}{$gvalue}=1;
			}
			achievementcheck ($server, $datetime, $roleid, "$datetime-$gvalue", \%hgshop, 1);
		}
    }
}
#   1   <=>     value   =>  date
#   2   <=>     date    =>  value
#   3   <=>     value compare directly

# Lib functions
# return number of quest completed
sub achievementcheck_mount {
	my ($server, $datetime, $roleid, $objects) = @_;
	my @tmparr = split (';', $objects);
	foreach my $obj_j1_j2 (@tmparr)
	{
		my ($itemid, $j1, $j2) = split(',', $obj_j1_j2);
		achievementcheck ($server, $datetime, $roleid, $itemid, \%hallmount, 1) if ($hmountreg{$itemid});
	}
}


sub get_occupation {
	my ($server, $datetime, $roleid) = @_;
	return if ($datetime !~ /....-..-.. ..:..:../);
	my $rsgs = "$server-$roleid-$game-stat";
	my %data = $cass->get('roleinfo', $rsgs);
	my $occ;
	foreach my $tmpdate (sort keys %{$data{$rsgs}})
	{
		last if ($datetime < $tmpdate);
		my $dataval = $data{$rsgs}{$tmpdate};
		my ($stat, $all) = ($dataval =~ m/(\w+),(.+)/);
		if ($stat eq 'created')
		{
			my ($userid, $race, $occupation, $gender, $city) = split (',', $all);
			$occ = $occupation;
		}
	} 
	return $occ if ($occ);
	return;
}

sub achievementcheck {
    my ($server, $datetime, $roleid, $value, $tmphash, $stat)=@_;
	return if ($datetime !~ /....-..-.. ..:..:../);
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
			my $deltime = $cass2->get_timestamp($datetime);
			$cass2->del('progress', "$server-$roleid-$ach", ($deltime+1)) if (($msg) and ($stat ne 3));
			print $msg;
		}
	}
}

sub achievementTask {
	my ($server, $datetime, $roleid, $taskid, $ach) = @_;
	my $max = $achievements{$ach}{'award_value'};
	print "updateProgress($ach,$server,$roleid,$datetime,$taskid,$datetime)";
	if ($hprogressdate{$ach})
	{	updateProgress($ach,$server,$roleid,$datetime,$datetime,$taskid);	}
	else 	
	{	updateProgress($ach,$server,$roleid,$datetime,$taskid,$datetime);	}

	my $have = getNumCompleted($ach,$server,$roleid,$datetime);
	if($have>=$max and $have>0 and $have<10000000 and $max>0 and $max<100000000) {
		my $msg = Util::completeAchievement($ach,$server,$roleid,$datetime);
		my $deltime = $cass2->get_timestamp($datetime);
		$cass2->del('progress', "$server-$roleid-$ach", ($deltime+1)) if ($msg);
		print $msg;
	}
}
	
sub getNumCompleted {
	my ($id, $server, $roleid, $datetime) = @_;
	return if ($datetime !~ /....-..-.. ..:..:../);

	my $count = $cass2->count('progress',"$server-$roleid-$id");
	my $msg = "count(progress, $server-$roleid-$id)";

	my $logDate = Util::get_epoch2date(time);
	print "$logDate $datetime $msg   => $count\n";

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
	print "$logDate $datetime $msg\n";
}
