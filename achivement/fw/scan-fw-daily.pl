#!/usr/bin/perl -I/apps/lib

use strict;
use warnings;
use Util;

die "Usage scan-pwi-daily.pl <perl script> <game> <path> <range>(yesterday|today|nextday|all) <type> <server>\n" if ($#ARGV != 5);

my ($script, $game, $path, $range, $type, $server) =@ARGV;
my @lsrange;

my $logPath = "/data2/logs/$game";

our $PROGRAM = $0; $PROGRAM =~ s|.*/||;
my ($day, $junk, $cfile);

my %hday = (
			'today'			=> 0,
			'yesterday' 	=> -86400,
			'nextday'		=> 86400,
			);

my %htype_convert = (	'log.gz'	=>	'log',
						'trace.gz'	=>	'trace',
						'chat.gz'	=>	'chat',
						'formatlog'	=>	'formatlog',
					);

my $ltype = $htype_convert{$type};

my %hlogtype = (
				'formatlog'	=>	"/data2/logs/today-$server--$game-$ltype.pl-done",
				'log'		=>	"/data2/logs/today-$server--$game-$ltype.pl-done",
				'chat'		=>	"/data2/logs/today-$server--$game-$ltype.pl-done",
				'trace'		=>	"/data2/logs/today-$server--$game-$ltype.pl-done",
				);

if ($range eq 'today' or $range eq 'yesterday' or $range eq 'nextday')
{	
	$day = Util::get_epoch2date(time + $hday{$range}); 
	($day, $junk) = split(' ',$day);
	$day =~ s/-/./g;
	@lsrange = `ls $path/$day*.$type `;
	$cfile = "today-$server--$script";
	if (!@lsrange)
	{
		`echo "FILE not availble: $day $junk - $type" > $logPath/$cfile`;
		die "File not availble: $day $junk - $type\n";
	}
	if ($range eq 'today')
	{
		my $checks = `grep "$day-$server-$type" $logPath/$cfile-done`;
		chomp $checks;
		die "Already processed : $script\n" if ($checks);
#		my $dfile = $hlogtype{$ltype};
#		my $logscript = "$game-$ltype.pl";
#		my $mverifyOne = `grep $day $dfile`;
#		chomp $mverifyOne;
#		die "Master log $logscript have not been imported.  Try another time !!\n" if (!$mverifyOne);
#		my $pcheck = `ps -ef | grep perl | grep $logscript | grep $day | grep -v grep | grep -v $PROGRAM | wc -l`;
#		chomp $pcheck;
#		die "Master log $logscript is processing.  Try another time !!\n" if ($pcheck != 0);
	}
} 
elsif ($range eq 'all')
{	@lsrange = `ls $path/*.$type`;	}
else {	@lsrange = `ls $path/$range*.$type`;	}


my $crange = @lsrange;
my $strprocess="ps -ef | grep $script | grep perl | grep -v grep | grep -vi zip | grep -v $PROGRAM";

my @exeprocess=`$strprocess`;
chomp @exeprocess;

my $count = @exeprocess;

while ($crange > 0)
{
	print "$count $crange\n";
	my $tfile = shift(@lsrange);
	chomp $tfile;
	my $ndate = `date '+%F %T'`;
	chomp $ndate;
	print "/usr/bin/perl /apps/$game/$script $tfile $server \n";
	`echo "$day-$server-$type" > $logPath/$cfile-done` if ($range eq 'today');
	`nohup /usr/bin/perl /apps/$game/$script $tfile $server > /dev/null 2> $logPath/$PROGRAM-$script-$range--$server-error & echo \$!`;
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
		my $currentdate = `date '+%F %T'`;
		chomp $currentdate;
		print "$currentdate $ndate Last script:$script , server:$server , range: $range , process count:$count\n";
		sleep 2;
	} while($count > 30);
	die if ($count eq 0 or $crange eq 0);
}
