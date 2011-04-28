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

#my ($ystart,$mstart,$dstart) = split /\W+/,$start;
#my ($yend,$mend,$dend) = split /\W+/,$end;

my $cassip = '172.29.1.116';
my $cassippro = '172.29.1.116';
my $cassipgam = '172.29.1.116';

my $dbhost = $cassip;
my $dbport = "9160";
my $keyspace = $server;

# Connect to the database.
my $cass = new CassandraPW( $dbhost, $dbport, $keyspace );
$cass->set_consistency( # set consistency levels.  default for read/write is ONE
    Net::Cassandra::Backend::ConsistencyLevel::ONE, # read
    Net::Cassandra::Backend::ConsistencyLevel::ONE  # write
);

my $casspro = new CassandraPW( $cassippro, $dbport, "cluster\_$server" );
$casspro->set_consistency( # set consistency levels.  default for read/write is ONE
    Net::Cassandra::Backend::ConsistencyLevel::QUORUM, # read
    Net::Cassandra::Backend::ConsistencyLevel::QUORUM  # write
);

my $cassgame = new CassandraPW( $cassipgam, $dbport, 'game_6' );
$cassgame->set_consistency( # set consistency levels.  default for read/write is ONE
    Net::Cassandra::Backend::ConsistencyLevel::QUORUM, # read
    Net::Cassandra::Backend::ConsistencyLevel::QUORUM  # write
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
		
	if ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) .+gamed: notice : formatlog:task:roleid=(\d+):taskid=(\d+):type=\d+:msg=finishtask,level=\d+,success=1,giveup=/) {
		my $key = "$2-task";
		my $col = "$1-formatlog-$count";
		my $val = $3;
		insertData($cass,'rolelogs',$key,$col,$val,$1);
	} elsif ($str =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}) database gamedbd: notice : formatlog:getrole:sid=\d+:userid=(\d+):roleid=(\d+):timestamp=\d+:level=(\d+):exp=\d+:money=(\d+)/) {
            # character changes roles on the login screen
            my ($datetime, $userid, $roleid, $level, $money) = ($1, $2, $3, $4, $5);
		my $key = $userid;
		my $col = $roleid;
		my $val = 1;
		insertData($cass, 'user2roles',$key,$col,$val,$datetime);	
		insertData($cassgame, 'user2roles',$key,"$server-$roleid-fw",$val,$datetime);	
		insertData($cassgame, 'roleinfo',"$server-$roleid-fw",'userid',$userid,$datetime);	
		$key = 'userid';
		$col = $userid;
		$val = 1;
		insertData($cass, 'index',$key,$col,$val,$datetime);	
	}

    $count++;
}

$logDateD = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
print "END: $logDateD --- SERVER: $server --- FILE: $file\n";

sub insertData {
	my ($casstmp, $cf, $key, $col, $val, $datetime) = @_;

	my %data = ( $col => $val );

	my $time = $casstmp->get_timestamp($datetime);
	my $logDate = strftime "%Y-%m-%d %H:%M:%S", localtime(time);
	$casstmp->set($cf,$key,\%data,$time);
	my $msg = "set($cf,$key, $col => $val)";
	print "$logDateD $logDate $datetime $msg\n";

}

