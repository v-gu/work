#!/usr/bin/perl -I/apps/lib

# Implementation of reading live log files inserting into MySQL
# Written by Greg Heckenbach
# 2010-04-30
# Modified by Arnold Domingo
# 2011-03-08

use POSIX qw(setsid);
use File::Tail::Multi;
use Date::Manip;
use Log::Dispatch;
use Log::Dispatch::FileRotate;
use Proc::PID::File;
use IO::Socket;
use Encode qw/decode/;
use HTML::Entities;
use Time::HiRes qw(usleep gettimeofday);
use WWW::Curl::Easy;
use WWW::Curl::Form;
use Digest::MD5 qw(md5_hex);
use serialize;
use Config::Tiny;
use CassandraPW;
use CassandraPW4;
use CassandraPW_Time;
use Util;
use strict;
use warnings;

#use Data::Dumper;

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

my %hoffsettime = (
                    39   =>  -3,
                    42   =>  0,
                    44   =>  -3,
                    46   =>  -9,
                    47   =>  -9,
                    48   =>  -9,
                    49   =>  -9,
                );
my $offsettime = $hoffsettime{$server};
my $SNSgame = "FW";

my $game = $hserver{$server};
die "Invalid server id\n" if (!$game);
my $logPath = "/data2/logs/$game/";

# PWESSANDRA CONFIG VARIABLES
my $dbhost = "172.29.1.165";
my $dbport = "9160";
my $keyspace = "game_6";

# Connect to the database.
my $cass = new CassandraPW_Time( $dbhost, $dbport, $keyspace );
$cass->set_consistency( # set consistency levels. default for read/write is QUORUM
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM,	# read
	Net::Cassandra::Backend::ConsistencyLevel::QUORUM	# write
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

		##########################################
		# A Kingdom is a Guild in Forsaken World #
		##########################################

		if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:kingdomadd:roleid=(\d+),level=\d+,kingdomid=(\d+)/) {
			# create guild
			my ($datetime, $roleid, $kingdomid) = ($1, $2, $3);
			my ($army, $rank) = (3, 1);
			processGuildInfo($server, $datetime, 'create', $roleid, $kingdomid, $army, $rank);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:kingdomjoin:kingdomid=(\d+),klevel=\d+,roleid=(\d+)/) {
			# someone joins the guild
			my ($datetime, $kingdomid, $roleid) = ($1, $2, $3);
			my ($army, $rank) = (1, 15);
			processGuildInfo($server, $datetime, 'join', $roleid, $kingdomid, $army, $rank);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:kingdomleave:kingdomid=(\d+),armyid=\d+,klevel=\d+,roleid=(\d+)/) {
			# someone leaves the guild
			my ($datetime, $kingdomid, $roleid) = ($1, $2, $3);
			processGuildInfo($server, $datetime, 'leave', $roleid, $kingdomid);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:kingdom:type=deleterole:roleid=(\d+):kingdomid=(\d+)/) {
			# user deletes their account
			my ($datetime, $roleid, $kingdomid) = ($1, $2, $3);
			processGuildInfo($server, $datetime, 'deleterole', $roleid, $kingdomid);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:delkingdom:kingdomid=(\d+),master=(\d+)/) {
			# guild is deleted after 7 days
			my ($datetime, $kingdomid, $roleid) = ($1, $2, $3);
			processGuildInfo($server, $datetime, 'delete', $roleid, $kingdomid);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:kingdomappoint:kingdomid=(\d+):armyid=(\d+):target=(\d+):kingdom_title=(\d+)/) {
			# guild member is promoted/demoted - new rank within same army
			my ($datetime, $kingdomid, $army, $target, $rank) = ($1, $2, $3, $4, $5);
			processGuildInfo($server, $datetime, 'promote', $target, $kingdomid, $army, $rank);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:kingdomchgarmy:kingdomid=(\d+):armyid=(\d+):dst_armyid=(\d+):roleid=(\d+),kingdom_title=(\d+)/) {
			# guild member is changing army
			# kingdomtitle = 1 => change leader ; new leader is in army 3, new leader will be leader of the army of new leader
			# army 1 : join guild but not in any army
			# army 2 : honor member
			# army 3+ : 1+
			my ($datetime, $kingdomid, $srcarmy, $dstarmy, $roleid, $kingdom_title) = ($1, $2, $3, $4, $5, $6);
			processGuildInfo($server, $datetime, 'chgarmy', $roleid, $kingdomid, $dstarmy, $kingdom_title, $srcarmy);

		} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:kingdomupgrade:roleid=(\d+),kingdomid=(\d+),level=(\d+)/) {
			# kingdom (guild) levels up
			my ($datetime, $roleid, $kingdomid, $level) = ($1, $2, $3, $4);
			processGuildInfo($server, $datetime, 'upgrade', $roleid, $kingdomid, $level);

		}
	}
}

# Lib functions
#

sub setIndex{
	my ($rsg, $gsg, $datetime, $type, $indexdata, $logDate, $time) = @_;
	my %rdata = ( $datetime	=> 	"$type,$gsg,$indexdata"	);
	$cass->set('index', "$rsg-role2guild", \%rdata, $time);
	my $msg = "set('index', $rsg-role2guild, $datetime => $type,$gsg,$indexdata :: $time";
	print "$logDate $datetime => $msg\n";

	my %gdata = ( $datetime	=> 	"$type,$rsg,$indexdata"	);
	$cass->set('index', "$gsg-guild2role", \%gdata, $time);
	$msg = "set('index', $gsg-guild2role, $datetime => $type,$rsg,$indexdata :: $time";
	print "$logDate $datetime => $msg\n";
}

sub processGuildInfo {
	my ($iserver, $datetime, $type, $roleid, $guildid, $army, $rank, $srcarmy)=@_;
	my $rsg="$iserver-$roleid-$game";
	my $gsg="$iserver-$guildid-$game";
	my $time = $cass->get_timestamp($datetime);
	my $logDate = Util::get_epoch2date(time);
	my %data;
	my $flagc=2;
	my $msg;

	# set index for guild and role
	my $indexdata = 0;
	$indexdata = "$army" if ($army);
	$indexdata = "$rank,$army" if ($rank);
	$indexdata = "$rank,$army,$srcarmy" if ($srcarmy);
	setIndex ($rsg, $gsg, $datetime, $type, $indexdata, $logDate, $time);

	if ($type eq "join")
	{
		my %roleinfo = ( 'guild' => $gsg, );
		$cass->set('roleinfo',$rsg,\%roleinfo,$time);
		$msg = "cass->set('roleinfo',$rsg, guild => $gsg, $time)";
		print "$logDate $datetime => $msg\n";
		%data = ($rsg	=> "$rank,$army,$type,$datetime", );
		$flagc = 1;
	
		sendSNS($datetime,$roleid,$iserver,$game,$guildid,$time);

	} elsif ($type eq "create") {
		my %roleinfo = ( 'guild' => $gsg, );
		$cass->set('roleinfo',$rsg,\%roleinfo,$time);
		$msg = "cass->set('roleinfo',$rsg, guild => $gsg, $time)";
		print "$logDate $datetime => $msg\n";
		%data = ( $rsg => "$rank,$army,join,$datetime",
					'level' => "1,$type,$datetime",
					'leader' => $rsg,
					'create' => $datetime,
					'officer' => '',
				);

		$flagc = 1;
		sendSNS($datetime,$roleid,$iserver,$game,$guildid,$time);

	} elsif (($type eq "leave") || ($type eq "deleterole")) {
		my %roleinfo = (
			'guild' => '',
			'lastguildjoin' => '',
		);
		$cass->set('roleinfo',$rsg,\%roleinfo,$time);
		$cass->del('guild',$gsg,($time+1),$rsg);
		$msg = "$gsg - $type: $rsg";
		print "$logDate $datetime => $msg\n";
		$flagc = 0;

	} elsif ($type eq "delete") {
		my %guildmember = $cass->get('guild',$gsg);
		return if (!$guildmember{$gsg});
		while((my $key, my $hvalue)=each(%{$guildmember{$gsg}}))
		{
			next if ($key =~ m/(level|leader|create|officer|name)/);
			my %roleinfo = (
				'guild' => '',
				'lastguildjoin' => '',
			);
			$cass->set('roleinfo',$key,\%roleinfo,($time+1));
			$msg = "cass->set('roleinfo',$key, guild => '', $time)";
			print "$logDate $datetime => $msg\n";
		}
		$cass->del('guild',$gsg,($time+1));
		$msg = "$gsg - $type -- DELETE GUILD";
		print "$logDate $datetime => $msg\n";
		$flagc = 0;

	} elsif ($type eq "promote") {
		# find out if guild is deleted first
		my %del = $cass->get('guild',$gsg);
		if(!%del or !defined $del{$gsg} or !defined $del{$gsg}{'create'}{'value'}) {
			return;
		}

		my %gdata = %del;
		if ($gdata{$gsg}{$rsg}{'value'})
		{
			my ($v,$a,$t,$rdate)=split(',',$gdata{$gsg}{$rsg}{'value'});
			$rdate = $datetime if ($rdate !~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/);
			%data = ( $rsg => "$rank,$army,join,$rdate" );
		} else {	%data = ( $rsg => "$rank,$army,join,$datetime" );	}

		$flagc = 1;

#			processGuildInfo($server, $datetime, 'chgarmy', $roleid, $kingdomid, $dstarmy, $kingdom_title, $srcarmy);
	} elsif ($type eq 'chgarmy') {
#print " ($iserver, $datetime, $type, $roleid, $guildid, $arsy, $rank, $srcarmy) \n";
		my %gdata = $cass->get('guild',$gsg);
		return if(!%gdata or !defined $gdata{$gsg} or !defined $gdata{$gsg}{'leader'}{'value'});
		return if ($gdata{$gsg}{'leader'}{'value'} eq $rsg);
		if ($gdata{$gsg}{$rsg}{'value'})
		{
			my ($v,$a,$t,$rdate)=split(',',$gdata{$gsg}{$rsg}{'value'});
			$rdate = $datetime if ($rdate !~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/);
			%data = ( $rsg => "$rank,$army,join,$rdate" );
		} else {	%data = ( $rsg => "$rank,$army,join,$datetime" );	}
		
		if ($rank eq 1)
		{
			$data{'leader'} = $rsg;
			my $ssg = $gdata{$gsg}{'leader'}{'value'};
			if ($gdata{$gsg}{$ssg}{'value'})
			{
				my ($v,$a,$t,$rdate)=split(',',$gdata{$gsg}{$ssg}{'value'});
				$rdate = $datetime if ($rdate !~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/);
				$data{$ssg} = "5,$srcarmy,join,$rdate";
			} else {	$data{$ssg} = "5,$srcarmy,join,$datetime";	}
		}
#print Dumper \%data;

		$flagc = 1;

	} elsif ($type eq 'upgrade') {
		my $level = $army;
		%data = ( 'level' => "$level,$type,$datetime", );
		$flagc = 1;
	}

	if ($flagc eq '1')
	{
		for my $key1 (sort keys %data){
			delete $data{$key1} if (($key1 eq "") || ($data{$key1} eq ""));
		}
		$cass->set('guild',$gsg,\%data, $time);
		$msg = "$gsg - $type: $rsg";
		print "$logDate $datetime => $msg\n";
	}
}

# send feed to SNS api
# roleid, server, and game are used to look up rolename if necessary
sub sendSNS {
	my ($date, $roleid, $server, $game, $guildid, $time) = @_;

	my $scanTime = time;
	my $logTime = Util::get_date2epoch($date) + $offsettime*3600;
	if(abs($scanTime-$logTime)>900) { 
		# only send SNS if this signal is within 15 minutes of scanning
		return;
	}
	$date = Util::get_epoch2date($logTime);

	# title or body contains [charname] - need to look up rolename and pass in %passdata
	my $gsg = "$server-$guildid-$game";
	my $rsg = "$server-$roleid-$game";
	my %data = $cass->get('roleinfo',$rsg);
	my $userid = $data{$rsg}{'userid'}{'value'};
	my $lastguildtime = 1;
	$lastguildtime = $data{$rsg}{'lastguildjoin'}{'timestamp'} if ($data{$rsg}{'lastguildjoin'}{'timestamp'});

	return if($time <= $lastguildtime);
	if(!$userid) {
		my $msg = "Need to send SNS feed, but feed requires userid and I can't find it in roleinfo[$rsg].\n";
		print $msg;
		my $logDate = Util::get_epoch2date(time);
		return;
	}

	my %roleinfo = ( 'lastguildjoin' => $date, );
	$cass->set('roleinfo',$rsg,\%roleinfo,$time);

	my $stime = UnixDate($date,"%s");
	# $userid from above
	my $code = '$Y*n6#Pb81Kg@C!';
	my $rand = int(rand(10000))+1;
	$stime = time;
	my $verify = substr(md5_hex($code.$userid.$stime.$rand),28);
	my $url = "http://core.perfectworld.com/api/sendfeed?i=$userid&t=$stime&r=$rand&v=$verify";

	my %passdata = (
		'notice_id' => 0,
		'roleid' => $roleid,
		'server' => $server,
		'game' => $game,
		'clanid' => $guildid,
	);

	my %postdata = (
		'title' => "[charname] has joined a guild on $SNSgame!",
		'body' => "[charname] is now a member of [clan].",
		'data' => serialize(\%passdata),
		'type' => 'joinguild'
	);
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

	my $msg = "called $url with ".serialize(\%passdata)." and ".serialize(\%postdata).", got $result";
	my $logDate = Util::get_epoch2date(time);
	print "$logDate $date $msg\n";
}

