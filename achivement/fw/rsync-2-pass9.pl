#!/usr/bin/perl -w
use POSIX qw(:signal_h :errno_h :sys_wait_h);
$SIG{CHLD} = 'IGNORE';


($server_id) = @ARGV;
die "Must input serverid\n" if (!$server_id);

# how many days in the past starting now
$days = 40;

$destination = "172.29.1.102::fwonetime/$server_id";
#local path
$origpath = "/data2/gamelogs2/fw/$server_id";

#max rsyncs to run

# files exts to send over, only for jd, eso, pwi, hotk
$exts = "world.formatlog|world.chat|world.trace|world.log";

my $ch = `ps -efa|grep rsync|grep -v "ps -efa"|grep progress`;
if ($ch =~ /$origpath/) {
    print "Recent Transfer\n";
    exit;
}

my @F = split("\/", $0);
my $myprogram = pop(@F);

open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
open STDOUT, ">>/var/log/$myprogram.rsyncs.log" or die "Can't write to log file /var/log/$myprogram.rsyncs.log $!";
open STDERR, ">>/var/log/$myprogram.rsyncs.log" or die "Can't write to /var/log/$myprogram.rsyncs.log $!";



# number of days in the last
my $ctime = time;
my %allDates = ();
my $daycount = 0;
while ($daycount < $days) {
    $daycount++;
    my ($day, $month, $year) = (localtime($ctime))[3..5];
    $year += 1900;
    $month++;
    my $ll = sprintf('%04d.%02d.%02d', $year, $month, $day);
    $allDates{$ll} = 1;
    $ctime -= (60 * 60 * 24);
}



our $PROGRAM = $0; $PROGRAM =~ s|.*/||;

my %allFiles = ();

$count = 0;
sub recurse($) {
  my($path) = @_;
  $path .= '/' if($path !~ /\/$/);
  for my $eachFile (glob($path.'*')) {
    ## if the file is a directory
    if(-d $eachFile) {
      ## pass the directory to the routine ( recursion )
      recurse($eachFile);
    } else {
        next if ($eachFile eq "." or $eachFile eq ".." or !($eachFile =~ /$exts/));
        my @filedate = split("-", $eachFile);
        $filedate[0] =~ s/$origpath|\///g;
        next if (!exists $allDates{"$filedate[0]"});
        $allFiles{$count} = "rsync -avc --progress $eachFile $destination";
        $count++;
    }
  }
}

recurse($origpath);
while(1) {
    my $filesNum = keys %allFiles;
    if ($filesNum eq 0) {
        exit;
    }
    my $rs = 0;
    #my @lines = `ps -efa|grep rsync|grep -v "ps -efa"|grep progress|grep 64.74.217.31| grep "\/data\/gamelogs\/22"|grep east3|grep EU`;
    my @lines = `ps -efa|grep rsync|grep -v "ps -efa"|grep progress`;
    #my @lines = `ps -efa|grep perl`;
    if ($rsyncs eq "") {
        $rsyncs = 30;
    }
    $rsyncs = int($rsyncs);

    my $rsync_procs = @lines;
    if ($rsync_procs < $rsyncs) {
        my $crs = $rsyncs - $rsync_procs;
        print "\n\n\n\nStats: Current Rsyncs: $rsync_procs   Max Limit: $rsyncs  Starting: $crs\n\n\n\n";
        sleep(2);
        while(my ($key, $value) = each(%allFiles)) {
            print "Processing $value\n";
            delete $allFiles{$key};
            if( 0 == fork() ) {
                exec($value);
                exit;
            }
            $rs++;
            if ($rs eq $crs) {
                last;
            }
        }
    }
    sleep(1);
}
