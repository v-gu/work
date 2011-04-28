#!/usr/bin/perl -I/apps/lib

# Implementation of reading live log files inserting into MySQL
# Written by Greg Heckenbach
# 2010-04-30

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
use MIME::Base64;
use Config::Tiny;
use File::Basename;

use CassandraPW;
use CassandraPW4;
use Util;
#use Util::Config;
use strict;
use warnings;

die "Usage file.pl <file> <server> \n" if ($#ARGV != 1);

my ($file, $server)=@ARGV;

#my ($ystart,$mstart,$dstart) = split /\W+/,$start;
#my ($yend,$mend,$dend) = split /\W+/,$end;

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


my $scriptPath  = "/apps/hotk"; 
my $Config      = Config::Tiny->new();
$Config         = Config::Tiny->read( $scriptPath. '/hotk_config.ini' );

my $item=$Config->{log}->{item};

#my %hitem = Util::Config::Convert2Hash($item);
#print Dumper \%hitem;

#die;


our $PROGRAM = $0; $PROGRAM =~ s|.*/||;
my $logDateD = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";

open(FILE, "gunzip -c $file |") or die "Can't open $file\n";
my %data=();
my $count=1;
while (<FILE>)
{
    chomp;
	my $str = $_;
	if($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: info : save_data:\d+发送保存用户(\d+)数据请求 包含仓库信息 等级(\d+) 金钱(\d+)/) {
		my $key = "$2-money";
		my $col = "$1-log-$count";
		my $val = "$3:$4";
		insertData('rolelogs',$key,$col,$val,$1);

=pod
	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: info : 用户(\d+)从数据库取得数据，职业(\d+),级别(\d+) 名字'([^']+)'/) {
			my ($datetime, $roleid, $occupation, $level, $encname) = ($1, $2, $3, $4, $5);
			my $name = decode_base64($encname);
			$name =~ s/\0//g;
			$name =~ tr/[A-Z]/[a-z]/;
			my $roleindex='rolename';
			if (($name =~ /^(\w\w).+/) or ($name =~ /^(\w)\W/))
			{
				$roleindex="$roleindex-".$1;
			}
			else {  $roleindex="$roleindex-misc";   }
			my $val = "$roleid:$occupation:$level";
			insertData('index', $roleindex, $name, $val, $datetime);
=cut

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: info : player_kill,killer=(\d+),killer_level=(\d+),killer_pkval=(\d+),victim=(\d+),victim_level=(\d+),victim_pkval=(\d+)/) {
			if ($2 ne $5) {
				my $key = "$2-kill";
				my $col = "$1-log-$count";
				my $val = "$5:$6:$7";
				insertData('rolelogs',$key,$col,$val,$1);
			
				$key = "$5-killed";
				$col = "$1-log-$count";
				$val = "$2:$3:$4";
				insertData('rolelogs',$key,$col,$val,$1);
			}
=pod
	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: info : stat:userid=(\d+):roleid=(\d+):offline=(\d+):timestamp=(\d+),level=(\d+):exp=(\d+):money=(\d+):cashused=(\d+):[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:ip=(.+)/) {
			my $key = "$3-stat";
			my $col = "$1-log-$count";
			my $val = "$2:$4:$5:$6:$7";
			insertData('rolelogs',$key,$col,$val,$1);
=cut

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: info : pethatch,roleid=(\d+),level=(\d+),petid=(\d+),petgid=(\d+-\d+)/) {
			my $key = "$2-pethatch";
			my $col = "$1-log-$count";
			my $val = "$3:$4:$5";
			insertData('rolelogs',$key,$col,$val,$1);
	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .+? gamed: info : petrename,roleid=(\d+),level=(\d+),petid=(\d+),petgid=(\d+-\d+),money=(\d+),bind_money=(\d+)/) {
			my $key = "$2-petrename";
			my $col = "$1-log-$count";
			my $val = "$3:$4:$5:$6:$7";
            insertData('rolelogs',$key,$col,$val,$1);
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
	my $msg = "set($cf,$key, $col => $val ,$time)";
	print "$logDate $datetime $msg\n";

	# now types

	my ($id,$type) = split('-',$key);

	return if !$type;
	return if ($id =~ /rolename/);

	my %tables = ( 'rolelogs' => 'roletypes', 'guildlogs' => 'guildtypes' );

	my $table = $tables{$cf};

	%data = ( $type => 1 );
	$cass->set($table,$id,\%data,$time);
	$msg = "set($table,$id, $type => 1 ,$time)";
	print "$logDate $datetime $msg\n";
}

