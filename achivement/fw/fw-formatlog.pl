#!/usr/bin/perl -I/apps/lib
#
# Modified by Arnold
#

use POSIX qw(setsid strftime);
use File::Tail::Multi;
use Date::Manip;
use Log::Dispatch;
use Log::Dispatch::FileRotate;
use Proc::PID::File;
use IO::Socket;
use Net::Cassandra;
use DateTime;
use Encode qw/decode/;
use HTML::Entities;
use serialize;
use Data::Dumper;
use DBI;
use DBManager;
use Switch;
use Time::HiRes qw(usleep gettimeofday);

use CassandraPW;
use CassandraPW4;
use Util;
use strict;
use warnings;

die "Usage file.pl <file> <server> \n" if ($#ARGV != 1);

my ($file, $server)=@ARGV;

#my ($ystart,$mstart,$dstart) = split /\W+/,$start;
#my ($yend,$mend,$dend) = split /\W+/,$end;

#my $cassip = '10.8.8.12';
#my $cassip = '192.168.8.150';
my $cassip = Util::get_cass_conf($server);

my $dbhost = $cassip;
my $dbport = "9160";
my $keyspace = $server;

# Connect to the database.
my $cass = new CassandraPW( $dbhost, $dbport, $keyspace );
$cass->set_consistency( # set consistency levels.  default for read/write is ONE
    Net::Cassandra::Backend::ConsistencyLevel::ONE, # read
    Net::Cassandra::Backend::ConsistencyLevel::ONE  # write
);

our $PROGRAM = $0; $PROGRAM =~ s|.*/||;
my $logDateD = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";


open(FILE, "<$file") or die "Can't open $file\n";
my %data=();
my $count=1;
while (<FILE>)
{
    chomp;
	my $str = $_;

	#
	# $key is always going to be "$roleid-whatever".
	# $col is always going to be "$date-formatlog-$count"
	# $val is always going to be "$all:$other:$fields"
	#

	if ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) backup glinkd-(\d+): notice : formatlog:rolelogin:userid=(\d+):roleid=(\d+):Sex=(\d+):level=(\d+):phyle=(\d+):profession=(\d+):peer_ip=([0-9\.]+)/) {
		my $key = "$4-login";
		my $col = "$1-formatlog-$count";
		my $val = "$3:$4:$6:$7:$8:$9";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) backup glinkd-\d+: notice : formatlog:login:account=([^:]+):userid=(\d+):sid=(\d+):peer=([0-9\.]+):peerport=(\d+)/) {
		my $key = "username";
        my $col = "$2";
		$col=~ tr/[A-Z]/[a-z]/;
        my $val = "$3";
        insertData('index',$key,$col,$val,$1);
        $key = 'userid2name';
        $col = "$3";
		$val = "$2";
        insertData('index',$key,$col,$val,$1);
        $key = 'userid';
        $col = "$3";
		$val = 1;
        insertData('index',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:getrole:sid=(\d+):userid=(\d+):roleid=(\d+):timestamp=(\d+):level=(\d+):exp=(\d+):money=(\d+):cash_used=(\d+)/) {
		my $key = "$4-logindetail";
		my $col = "$1-formatlog-$count";
		my $val = "$2:$6:$7:$8:$9";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) backup glinkd-\d+: notice : formatlog:rolelogout:userid=(\d+):roleid=(\d+):localsid=(\d+):time=(\d+)/) {
		my $key = "$3-logout";
		my $col = "$1-formatlog-$count";
		my $val = "$4:$5";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:putrole:sid=(\d+):userid=(\d+):roleid=(\d+):timestamp=(\d+):level=(\d+):exp=(\d+):money=(\d+):cash_used=(\d+)/) {
		my $key = "$4-logoutdetail";
		my $col = "$1-formatlog-$count";
		my $val = "$2:$6:$7:$8:$9";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:createrole:userid=(\d+):roleid=(\d+):city=(\d+):race=(\d+),occupation=(\d+):gender=(\d+)/) {
		my $key = "$3-created";
		my $col = "$1-formatlog-$count";
		my $val = "$2:$4:$5:$6:$7";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$2";
		$col = "$3";
		$val = 1;
		insertData('user2roles',$key,$col,$val,$1);
		$key = 'userid';
		$col = "$2";
		insertData('index',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:kingdomupgrade:roleid=(\d+),kingdomid=(\d+),level=(\d+),population=(\d+),op_type=(\d+)/) {
		my $key = "$2-level";
		my $col = "$1-formatlog-$count";
		my $val = "$3:$4:$5:$6";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: notice : formatlog:die:roleid=(\d+):type=(\d+):attacker=(\d+)/) {
		my $key = "$2-pvp";
		my $col = "$1-formatlog-$count";
		my $val = "$2:$4";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$4-pvp";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: notice : formatlog:task:roleid=(\d+):taskid=(\d+):type=\d+:msg=finishtask,level=(\d+),success=1,giveup=0,gold=(\d+),bindmoney=(\d+),exp=(\d+),itemid:count=(.*)/) {
		my $itemlist = $8;
		if ($itemlist) {
			$itemlist =~ s/:/-/g;
		} else {
			$itemlist = "0-0";
		}
		my $key = "$2-task";
		my $col = "$1-formatlog-$count";
		my $val = "$3:$4:$5:$6:$7:$itemlist";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: notice : formatlog:gshop_trade:userid=(\d+):roleid=(\d+):order_id=(\d+):item_id=(\d+):expire=(\d+):item_count=(\d+):cash_need=(\d+):guid=([^:]+):reputation=(.+)/) {
		my $key = "$2-gshop";
		my $col = "$1-formatlog-$count";
		my $val = "$3:$4:$5:$6:$7:$8:$9:$10";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:kingdomadd:roleid=(\d+),level=(\d+),kingdomid=(\d+),money=(\d+),bindmoney=(\d+)/) {
		my $key = "$2-guild";
		my $col = "$1-formatlog-$count";
		my $val = "create:$3:$4:$5:$6";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$4-create";
		$val = "create:$2";
		insertData('guildlogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:kingdomjoin:kingdomid=(\d+),klevel=(\d+),roleid=(\d+),level=(\d+)/) {
		my $key = "$4-guild";
		my $col = "$1-formatlog-$count";
		my $val = "join:$2:$3";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$2-join";
		$val = "join:$4:$5";
		insertData('guildlogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:kingdomleave:kingdomid=(\d+),armyid=(\d+),klevel=(\d+),roleid=(\d+),level=(\d+)/) {
		my $key = "$5-guild";
		my $col = "$1-formatlog-$count";
		my $val = "leave:$2:$3:$4";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$2-leave";
		$val = "leave:$5:$6";
		insertData('guildlogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:kingdom:type=deleterole:roleid=(\d+):kingdomid=(\d+):kingdom_title=(\d+):kbase_title=(\d+)/) {
		my $key = "$2-guild";
		my $col = "$1-formatlog-$count";
		my $val = "deleterole:$3:$5";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$3-deleterole";
		$val = "deleterole:$2:$4";
		insertData('guildlogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:kingdomappoint:kingdomid=(\d+):armyid=(\d+):target=(\d+):kingdom_title=(\d+)/) {
		my $key = "$4-guild";
		my $col = "$1-formatlog-$count";
		my $val = "promote:$2:$3:$5";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$2-promote";
		$val = "promote:$4:$5";
		insertData('guildlogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:kingdomchgarmy:kingdomid=(\d+):armyid=(\d+):dst_armyid=(\d+):roleid=(\d+),kingdom_title=(\d+)/) {
		my ($datetime, $kingdomid, $srcarmy, $dstarmy, $roleid, $kingdom_title) = ($1, $2, $3, $4, $5, $6);
		my $key = "$roleid-guild";
		my $col = "$datetime-formatlog-$count";
		my $val = "chgarmy:$kingdomid:$srcarmy:$dstarmy";
		insertData('rolelogs',$key,$col,$val,$1);
		$key = "$kingdomid-chgarmy";
		$val = "chgarmy:$roleid:$kingdom_title";
		insertData('guildlogs',$key,$col,$val,$1);


	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:delkingdom:kingdomid=(\d+),master=\d+,population=\d+/) {
		my $key = "$2-delete";
		my $col = "$1-formatlog-$count";
		my $val = 1;
		insertData('guildlogs',$key,$col,$val,$1);

	#
	# $key is always going to be "$roleid-whatever".
	# $col is always going to be "$date-formatlog-$count"
	# $val is always going to be "$all:$other:$fields"
	#

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) backup gdeliveryd: notice : formatlog:trade:roleidA=(\d+):roleidB=(\d+):moneyA=(\d+):moneyB=(\d+):objectsA=([^:]*):objectsB=(.*)/) {
		my ($datetime, $src_role, $dst_role, $src_money, $dst_money, $src_obj, $dst_obj) = ($1, $2, $3, $4, $5, $6, $7);
		my $srckey = "$src_role-trade";
		my $srccol = "$datetime-formatlog-$count";
		my $srcval = "$src_money:$src_obj";
		insertData('rolelogs',$srckey,$srccol,$srcval,$1);
		my $dstkey = "$dst_role-trade";
		my $dstcol = "$datetime-formatlog-$count";
		my $dstval = "$dst_money:$dst_obj";
		insertData('rolelogs',$dstkey,$dstcol,$dstval,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+? gamed: notice : levelup,roleid=(\d+),level=(\d+),money=(\d+)/) {
		my ($datetime, $roleid, $level, $money) = ($1, $2, $3, $4);
		my $key = "$roleid-levelup";
		my $col = "$datetime-formatlog-$count";
		my $val = "$level:$money";
		insertData('rolelogs',$key,$col,$val,$1);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+? gamed: notice : itemtrade,roleid=(\d+),level=(\d+),itemid=(\d+),count=\d+,bind_money=(\d+),money=(\d+)/) {
		my ($datetime, $roleid, $level, $itemid, $bind_money, $money) = ($1, $2, $3, $4, $5, $6);
		my $key = "$roleid-repmount";
		my $col = "$datetime-formatlog-$count";
		my $val = "$level:$itemid:$count:$bind_money:$money";
		insertData('rolelogs',$key,$col,$val,$datetime);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+? gamed: notice : refineitem,roleid=(\d+),level=(\d+),itemid=(\d+),equip_mask=(\d+),stone1=\d+,stone1_count=\d+,stone2=\d+,stone2_count=\d+,money=\d+,bind_money=(\d+),result=1,equip_level=(\d+)/) {
		my ($datetime, $roleid, $level, $itemid, $equip_mask, $bind_money, $equip_level) = ($1, $2, $3, $4, $5, $6, $7);
		my $key = "$roleid-refineitem";
		my $col = "$datetime-formatlog-$count";
		my $val = "$level:$itemid:$equip_mask:$bind_money:$equip_level";
		insertData('rolelogs',$key,$col,$val,$datetime);

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+? gamed: notice : petincubate,userid=(\d+),petid=(\d+),petgid=([^,]+),quality=(\d+),egg_id=(\d+),egg_quality=(\d+),time=(\d+)/) {
		my ($datetime, $roleid, $petid, $petgid, $quality, $egg_id, $egg_quality, $time) = ($1, $2, $3, $4, $5, $6, $7, $8);
		my $key = "$roleid-petincubate";
		my $col = "$datetime-formatlog-$count";
		my $val = "$petid:$petgid:$quality:$egg_id:$egg_quality:$time";
		insertData('rolelogs',$key,$col,$val,$datetime);


	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+? gamedbd: notice : formatlog:sendmail:timestamp=(\d+):src=(\d+):src_level=\d+:dst=(\d+):dst_level=\d+:mid=\d+:size=\d+:money=(\d+):item=(\d+):count=\d+:pos=\d+/) {
		my ($datetime, $timestamp, $roleid, $dst, $money, $item) = ($1, $2, $3, $4, $5, $6);
		my $key = "$roleid-sendmail";
		my $col = "$datetime-formatlog-$count";
		my $val = "$timestamp:$dst:$money:$item:$count";
		insertData('rolelogs',$key,$col,$val,$datetime);

	}

    $count++;
}

$logDateD = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
print "END: $logDateD --- SERVER: $server --- FILE: $file\n";

sub insertData {
	my ($cf, $key, $col, $val, $datetime) = @_;

	my %data = ( $col => $val );

	my $time = $cass->get_timestamp($datetime);
	my $logDate = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
	$cass->set($cf,$key,\%data,$time);
	my $msg = "set($cf,$key, $col => $val)";
	print "$logDateD $logDate $datetime $msg\n";

	# now types

	my ($id,$type) = split('-',$key);

	return if !$type;

	my %tables = ( 'rolelogs' => 'roletypes', 'guildlogs' => 'guildtypes', 'alliancelogs' => 'alliancetypes' );

	my $table = $tables{$cf};

	%data = ( $type => 1 );
	$cass->set($table,$id,\%data,$time);
	$msg = "set($table,$id, $type => 1)";
	print "$logDateD $logDate $datetime $msg\n";
}
