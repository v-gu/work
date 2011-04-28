#!/usr/bin/perl -I/apps/lib

use strict;
use warnings;
use Util;

die "Usage fw-logcheck.pl <perl script> <game> <path> <range>|today|all <type> <server>\n" if ($#ARGV != 5);

my ($script, $game, $path, $range, $type, $server) =@ARGV;
my @lsrange;

our $PROGRAM = $0; $PROGRAM =~ s|.*/||;
my ($today, $junk, $cfile);
if ($range eq 'today')
{	
 	$today = Util::get_epoch2date(time);
	($today, $junk) = split(' ',$today);
	$today =~ s/-/./g;
	@lsrange = `ls $path/$today*.$type `;
	$cfile = "today-$server--$script";
	if (!@lsrange)
	{
		`echo "FILE not availble: $today $junk - $type" > /data2/logs/$cfile`;
		die;
	}
	`touch /data2/logs/$cfile-done`;
	my $checks = `grep "$today-$server-$type" /data2/logs/$cfile-done`;
	chomp $checks;
	die "ALREADY RUN\n" if ($checks);
}
elsif ($range eq 'all')
{	@lsrange = `ls $path/*.$type`;	}
else {	@lsrange = `ls $path/$range*.$type`;	}


my $crange = @lsrange;
my $strprocess="ps -ef | grep $script | grep perl | grep -v grep | grep -vi zip";

my @exeprocess=`$strprocess`;
chomp @exeprocess;

my $count = @exeprocess;

while ($crange > 0)
{
	print "$count $crange\n";
	my $tfile = shift(@lsrange);
	chomp $tfile;
	print "/usr/bin/perl /apps/$game/$script $tfile $server \n";
	`echo "$today-$server-$type" > /data2/logs/$cfile-done` if ($range eq 'today');
	`nohup /usr/bin/perl /apps/$game/$script $tfile $server > /dev/null 2> /data2/logs/$PROGRAM-$script-$range--$server-error & echo \$!`;
#	`/usr/bin/perl /apps/$game/$script $tfile $server`;
#	print "$printout";
	sleep 2;
	do {
#		my $filesize = `ls -al /data1/logs/$0-$server | awk '{print \$5}'`;
#		chomp $filesize;
#		print "$filesize\n";
		@exeprocess=`$strprocess`;
		$count = @exeprocess;
		$crange = @lsrange;
		print "still have $count running - sleep\n";
		sleep 2;
	} while($count > 50);
	die if ($count eq 0 or $crange eq 0);
}
