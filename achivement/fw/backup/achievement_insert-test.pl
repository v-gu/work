#!/usr/bin/perl -I/apps/lib

use Net::Cassandra;

use Switch;
use Time::HiRes qw(usleep gettimeofday);
use Data::Dumper;

use CassandraPW;
use strict;
use warnings;

my $file = "achieve-raw-fw.txt";
# CASS CONFIG VARIABLES
my $Chost   = "172.29.1.161";
my $Cport   = "9160";
my $keyspace = "game_6";

# Connect to pwessandra
my $cass = new CassandraPW( $Chost, $Cport, $keyspace );
$cass->set_consistency( # set consistency levels.  default for read/write is ONE
        Net::Cassandra::Backend::ConsistencyLevel::QUORUM, # read
        Net::Cassandra::Backend::ConsistencyLevel::QUORUM  # write
);

# Read file

my $httpurl="http://fw-forum.perfectworld.com/templates/common/images/avatars/BT";
my $scriptname="scan_achivement_fw.pl";
my ($pointvalue, $name, $descr);

open (FILE, "<$file") or die "Can't open $file\n";

my $count = 386;

my $game = "Forsaken World";
my $fbtitle = "[fb_name] just received an achievement in Perfect World International!";
my $fbname = "[ach_name]<br>[ach_desc]";
my %hclass10 	= (	386	=> 1, 
					390	=> 1,
					394	=> 1,
					398	=> 1,
					402	=> 1,
					406	=> 1,
					410	=> 1,
					414	=> 1,
				);
my %hclass30 	= (	 
387 => 1, 
391 => 1,
395 => 1,
399 => 1,
403 => 1,
407 => 1,
411 => 1,
415 => 1,
				);
my %hclass50 	= (	 
388 => 1,  
392 => 1, 
396 => 1, 
400 => 1, 
404 => 1, 
408 => 1, 
412 => 1, 
416 => 1, 
				);
my %hclass70 	= (	 
389 => 1, 
393 => 1,
397 => 1,
401 => 1,
405 => 1,
409 => 1,
413 => 1,
417 => 1,
				);

#$cass->del('index', 'achievementid');

while (<FILE>)
{
	chomp;
#	$cass->del('data', $count);

	my ($name, $token, $desc, $point, $category, $min, $max);
	if (/(.+)\t(.+)\t(.+)\t(.+)\t(\d+) - (\d+)/g) #\t(.+)\t(.+)--(.+)/g)
	{
		($category, $point, $name, $desc, $min, $max) = ($1, $2, $3, $4, $5, $6);
	}
	$name =~ s/\t//g;
	$desc =~ s/\t//g;
	$point =~ s/\t//g;
	$category =~ s/\t//g;
	$min =~ s/\t//g;
	$max =~ s/\t//g;

#die "$_      $category, $point, $name, $desc, $min, $max\n" if ($count eq 434);
	
	print "added key $count, desc=$desc\n";
	my %data = (
			game => $game,
			name => $name,
			description => $desc,
			image => $httpurl,
			point => $point,
			category => $category,
			min_value => $min,
			show_min_value => 0,
			award_value => $max,
			has_details => 0,
			feed_title => $fbtitle,
			feed_body	=> $fbname,
			script => $scriptname
	);
#	$data{'parent_id'} = 418 if ($hclass10{$count});
#	$data{'children_id'} = '386,390,394,398,402,406,410,414' if ($count == 418);
#	$data{'parent_id'} = 419 if ($hclass30{$count});
#	$data{'children_id'} = '387,391,395,399,403,407,411,415' if ($count == 419);
#	$data{'parent_id'} = 420 if ($hclass50{$count});
#	$data{'children_id'} = '388,392,396,400,404,408,412,416' if ($count == 420);
#	$data{'parent_id'} = 421 if ($hclass70{$count});
#	$data{'children_id'} = '389,393,397,401,405,409,413,417' if ($count == 421);
	$cass->set('data', $count, \%data);

	my %indexdata = ( $count	=>	$name,	);
	$cass->set('index', 'achievementid', \%indexdata);
#print Dumper \%data;
	$count++;
}
