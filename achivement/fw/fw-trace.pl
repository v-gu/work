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

use CassandraPW;
use CassandraPW4;
use Util;
use strict;
use warnings;

die "Usage file.pl <file> <server> \n" if ($#ARGV != 1);

my ($file, $server)=@ARGV;

my $filetype = "trace";

#my ($ystart,$mstart,$dstart) = split /\W+/,$start;
#my ($yend,$mend,$dend) = split /\W+/,$end;

our $PROGRAM = $0; $PROGRAM =~ s|.*/||;
my $logDateD = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
print "START: $logDateD --- SERVER: $server --- FILE: $file\n";

open(FILE, "gunzip -c $file |") or die "Can't open $file\n";
my %data=();
my %dataroletype = ();
my %dataguild=();
my %dataguildtype=();
my $count=1;
my $datetime = '';
while (<FILE>)
{
    chomp;
	my ($zoneid, $roleid, $name, $factionid, $kingdomid, $userid);
	my $str = $_;
    if ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) backup unamed: debug : unamed::ConfirmRoleName: result=\d+,zoneid=(\d+),userid=(\d+),roleid=(\d+),rolename="([^"]+)"/) {
		($datetime, $zoneid, $userid, $roleid, $name) = ($1, $2, $3, $4, $5);
		$data{$zoneid}{'rolelogs'}{"$roleid-created"}{"$datetime-$filetype-$count"} = "$name:$userid";
		$data{$zoneid}{'roletypes'}{$roleid}{'created'}=1;
		
	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) backup unamed: debug : unamed::ConfirmName: result=\d+,zoneid=(\d+),category=\d+,id=(\d+),name="([^"]+)"/) {
		($datetime, $zoneid, $kingdomid, $name) = ($1, $2, $3, $4);
		$data{$zoneid}{'guildlogs'}{"$kingdomid-create"}{"$datetime-$filetype-$count"}=$name;
		$data{$zoneid}{'guildtypes'}{$kingdomid}{'create'}=1;
		my $gindex='guildname';
		$name =~ tr/[A-Z]/[a-z]/;
		$data{$zoneid}{'index'}{'guildname'}{$name}=$kingdomid;

	} elsif ($str =~ /(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) backup unamed: notice : ReleaseRoleName, zoneid=(\d+):userid=(\d+):roleid=(\d+):rolename="([^"]+)"/) {
		($datetime, $zoneid, $userid, $roleid, $name) = ($1, $2, $3, $4, $5);
		$data{$zoneid}{'rolelogs'}{"$roleid-deleted"}{"$datetime-$filetype-$count"} = "$name:$userid";
		$data{$zoneid}{'roletypes'}{$roleid}{'deleted'}=1;

	}

    $count++;
}

my $keyspace;
my $cassip;
my $dbhost;
my $dbport;
my $cass;
my $time;

foreach my $zkey (sort keys %data) {
	my $tmp = $data{$zkey};
	my %tmp2 = %$tmp;

	$keyspace = $zkey;

	# Connect to the database.
	$cassip = Util::get_cass_conf($server);

	$dbhost = $cassip;
	$dbport = "9160";

	$cass = new CassandraPW( $dbhost, $dbport, $keyspace );
	$cass->set_consistency( # set consistency levels.  default for read/write is ONE
		Net::Cassandra::Backend::ConsistencyLevel::ONE, # read
		Net::Cassandra::Backend::ConsistencyLevel::ONE  # write
	);
	# Connect to the database.

	$time = $cass->get_timestamp($datetime);

	foreach my $tkey (keys %tmp2) {
		my $tmp3 = $tmp2{$tkey};
		my %tmp4 = %$tmp3;

		my $colkingdom = $tkey;

		foreach my $kkey (keys %tmp4) {
			my $tmp5 = $tmp4{$kkey};
			my %ins = %$tmp5;

			my $datastr = '';
			foreach my $ckey (keys %ins) {
				$datastr .= "$ckey => $ins{$ckey} ,";
			}

			my $logDate = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
			$cass->set($colkingdom,$kkey,\%ins,$time);
			my $msg = "set($colkingdom,$kkey, $datastr$time)";
			print "$logDate $datetime $msg\n";

		}
	}
	undef $keyspace;
	undef $cassip;
	undef $dbhost;
	undef $dbport;
	undef $cass;
	undef $time;
}

$logDateD = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
print "END: $logDateD --- SERVER: $server --- FILE: $file\n";
