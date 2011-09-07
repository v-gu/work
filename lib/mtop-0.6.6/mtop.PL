#!/usr/local/bin/perl

#  (DO NOT EDIT mtop, instead edit mtop.PL and run make

use Config;
use File::Basename qw(&basename &dirname);
use Getopt::Long;
use strict;
use vars ( qw( $opt_v $opt_r ) );

GetOptions(
    "version=s"=>\$opt_v,
    "release=s"=>\$opt_r,
);

# This forces PL files to create target in same directory as PL file.
# This is so that make depend always knows where to find PL derivatives.
chdir(dirname($0));
my $file;
($file = basename($0)) =~ s/\.PL$//;
$file =~ s/\.pl$//
        if ($Config{'osname'} eq 'VMS' or
            $Config{'osname'} eq 'OS2');  # "case-forgiving"

open OUT,">$file" or die "Can't create $file: $!";
print "Extracting $file (with variable substitutions)\n";

print OUT <<"!GROK!THIS!";
$Config{'startperl'}

\$VERSION = "$opt_v";
\$RELEASE = "$opt_r";

!GROK!THIS!

# In the following, perl variables are not expanded during extraction.

#  (DO NOT EDIT mtop, instead edit mtop.PL and run make

print OUT <<'!NO!SUBS!';
#
# $Id: mtop.PL,v 1.61 2004/09/12 22:22:03 mdprewitt Exp $
#
# mtop - Shows the MySQL commands consuming the greatest time
# Copyright (C) 2002 Marc Prewitt/Chelsea Networks <mprewitt@chelsea.net>
# 
# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
# 
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
# for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
# 
#

=head1 NAME 

B<mtop> - Shows top mysql threads

=head1 SYNOPSIS

    mtop [--host={mysql_host}] [--dbuser={mysql_user}] 
        [--password={mysqluser_pw}] [--seconds={refresh}] [--[no]idle] 
        [--filter-user={regex}] [--filter-host={regex}] [--filter-db={regex}]
        [--filter-command={regex}] [--filter-state={regex}] [--filter-info={{regex}}]
        [--fold-select-columns]
        [--user={user}] [--manualrefresh] [--slow={seconds}] [--vs|veryslow={seconds}]
        [--vvs|veryveryslow={seconds}]

    mtop --help

    mtop --version

=head1 DESCRIPTION

Shows the MySQL commands consuming the greatest time.  By default, only non-sleeping 
threads are shown, the B<--idle> option shows idle threads.  While running several 
keys will affect the operation of B<mtop>.  Hitting B<h> or B<?> will show the 
available options.

Normally, run as a console program this will allow you to see errant or badly 
optimized queries as they will stay on the screen for a while.  However, if you
are hunting for short lived queries, running in the B<manualrefresh> mode with a 
short refresh time will allow you to catch short lived queries as well.

The following keys are active while B<mtop> is running:

    q - quit
    ? - help

    Filtering/display

    s - change the number of seconds to delay between updates
    m - toggle manual refresh mode on/off
    d - filter display with regular expression (user/host/db/command/state/info)
    F - fold/unfold column names in select statement display
    h - display process for only one host
    u - display process for only one user
    i - toggle all/non-Sleeping process display
    o - reverse the sort order

    Control/Detail

    k - kill processes; send a kill to a list of ids
    e - explain a process; show query optimizer info
    z - zoom in on a process; show sql statement detail 
    f - flush stats (reset show status variables)
    t - show mysqld stats (show status/mysqladmin ext)
    T - show short/important status
    v - show mysqld variables (show variables/mysqladmin vars)
    r - show replication status for master/slaves

B<Main Screen>

The main query screen shows the following information as well as the currently
active queries (explanations are from the MySQL online manual and 
references refer to the section in the manual where the explanation came from):

=over 4

=item n Threads: running, cached

The B<n> Threads represents how many threads the mysqld has allocated.  One
thread is allocated for each user connection.  Additional threads are allocated
for replication.

=item    Queries/slow: Total queries / Total SLOW QUERIES

The first number is the total number of queries sent to the server
since the last 'flush status' or since server start.  The second
number is the number of queries that have taken more than 
long_query_time. See section 4.9.5 The Slow Query Log. 

=item    Cache Hit: Cache hit ratio

This is the percentage of times a key read is handled from the 
key buffer cache.  See section 4.5.7.4 B<SHOW VARIABLES> of 
the MySQL manual for more information.

=item    Opened tables: tables opened

MySQL has a cache for open tables.  If 'opened tables' is high, your cache
may be too small.  Look at the MySQL manual section: B<5.4.7 How MySQL Opens 
and Closes Tables> for further information.

=item    RRN: Handler_read_rnd_next

Number of requests to read the next row in the datafile. This will be 
high if you are doing a lot of table scans. Generally this suggests 
that your tables are not properly indexed or that your queries are 
not written to take advantage of the indexes you have. (4.5.7.3)

=item    TLW: Table_locks_waited

Number of times a table lock could not be acquired immediately and a 
wait was needed. If this is high, and you have performance problems, 
you should first optimise your queries, and then either split your 
table(s) or use replication. Available after 3.23.33.  (4.5.7.3)

=item    SFJ: Select_full_join

Number of joins without keys (If this is not 0, you should carefully 
check the indexes of your tables). (4.5.7.3)

=item    SMP: Sort_merge_passes

Number of merges passes the sort algoritm have had to do. If this value 
is large you should consider increasing sort_buffer. (4.5.7.3)

=item    QPS: Questions per second

The total number of sql commands handled by the MySQL server since startup
or the last B<flush status> command.

=back

B<Statistics/Variables>

When viewing the I<stats> screen (B<t>), the screen will refresh until a key is 
pressed at which point you will return to the main screen.  The bottom of the 
I<stats> screen is denoted with a line containing B<--->.  If you do not see
that line, resize your screen until you do.

The statistics screen has the following format:

    Stat:      total [avg per sec / instant per sec ]

For example:

    Questions:     720,672 [30/12]

The I<short/important> status screen is a list of recommendations from the
MySQL manual.  

The first number is the total since startup or the last 'flush status'. 
The second number is the number per second since startup or flush.  The
last is the number per second since the last screen refresh.  

The I<variables> screen only shows the information once and returns to the main 
screen as the variables do not change after server startup.

B<Replication>

The replication monitor screen looks for a master or slave server running on the currently
monitored mysqld.  If a master server is found, it then tries to connect to each slave
connected to the master.  Replication is shown for all masters and slaves found.
Offsets from the master for each of the slaves is shown.  Note: the offset may be less
than zero because the slave position is checked after the master position.  The offset
shown is the number of queries in the binlog that the slave has to process before
being caught up with the master.

=head1 OPTIONS

All options can be abbreviated by their shortest unique abbreviation.

=over 4

=item -?, --help  

Show the help screen and exit.

=item -v, --version  

Show the version number and exit.

=item -h {mysql_host}, --host={mysql_host}  

By default, the mysqld on localhost is monitored.  Specify an alternate host
with this option.

=item -dbu {mysql_user}, --dbuser={mysql_user}

By default, the user 'mysqltop' is used to connect to the database.  Specify an alternate user with this option.

=item -p {mysqluser_pw}, --password={mysqluser_pw}

By default, there is no password associated with the mysqltop
user, specify a password with this option.

=item -se {refresh}, --seconds={refresh}

The default screen refresh is 5 seconds.

=item -sl {seconds}, --slow={seconds}

The number of seconds before a slow query is highlighted.  The default is
the server's long_query configuration variable.

=item -vs {seconds}, --veryslow={seconds}

The number of seconds before a very slow query is highlighted.  The default is
the the --slow option * 2.

=item -vvs {seconds}, --veryveryslow={seconds}

The number of seconds before a very very slow query is highlighted.  The default is
the the --slow option * 4.

=item -i, --[no]idle 

By default, processes in the B<Sleep> command state are not shown.  This option turns
on display of idle threads.

=item -u {user}, --user={user}

Show only threads owned by this user.

=item -fu {regex_pattern}, --filter-user={regex_pattern}

=item -fh {regex_pattern}, --filter-host={regex_pattern}

=item -fd {regex_pattern}, --filter-db={regex_pattern}

=item -fs {regex_pattern}, --filter-state={regex_pattern}

=item -fc {regex_pattern}, --filter-command={regex_pattern}

=item -fi {regex_pattern}, --filter-info={regex_pattern}

Filter the display based on the B<regex_pattern> provided.  The B<regex_pattern> is a perl
regular expression.  The regular expression match is done with case insensitivity.

For example, to only show B<select> statements on the B<user> table, use the following:

    --filter-info='select from user'

or, to be more forgiving for mutil-table joins and extra spaces, use:

    --filter-info='select\s+from\s+.*\buser\b.*where'

These same regular expression filters can be used with the interactive B<d> command.
Be careful to escape any special shell characters in the regex.

=item -m, --manualrefresh 

In this mode, the screen only refreshes when the user hits a key on the
keyboard.  The screen will refresh automatically until a query is seen and then wait for 
further input.  An uppercase M will appear in the top right hand corner of the screen to 
indicate that you are in this mode.

=back

All options can be stored in initialization files.  Command line options override
options stored in the initialization file(s).  The following files are checked for
arguments: current direcotry .mtoprc, home directory .mtoprc, /usr/local/etc/mtoprc, 
/etc/mtoprc.  Options in the former files override options in the later files.

The format of the initialization file is one option per line.  Options are specified just
as they would be on the command line.  They can be abbreviated and use the one or two hyphen
syntax.  Comments and blank lines are ignored.  The following is an exmple .mtoprc file 
which sets a user filter to user1 and sets the refresh rate to one second:

    #  Only look at 'user1'
    -fu user1
    --seconds=1   # refresh every one seconds

=head1 SETUP

The most convenient way to setup your system to use B<mtop> is to create a database user
called B<mysqltop> which has no password.  For security purposes, this user should have 
all privileges set to B<N> except B<Process_priv> which must be set to B<Y>.

To grant these privileges, execute the following from the MySQL command prompt

For mysql 4.0.2 and greater:

    mysql> grant super, reload, process on *.* to mysqltop;
    mysql> grant super, reload, process on *.* to mysqltop@localhost;
    mysql> flush privileges;


For mysql 3.x and 4.0.1:

    mysql> grant reload, process on *.* to mysqltop;
    mysql> grant reload, process on *.* to mysqltop@localhost;
    mysql> flush privileges;

Notes: 

=over 4

=item 

GRANT only works in MySQL 3.22.11 or later, for earlier versions add the user
manually and fix the permissions as noted above.  

=item 

The GRANT to mysqltop and mysqltop@localhost may be modified depending upon which 
hosts you want to grant access from.  In general, you probably want to limit it to 
the hosts in your domain.  

=item 

The B<reload> privilege is not required for B<mtop> to do basic monitoring.  It is 
only needed if you wish to issue B<flush> commands from B<mtop>.  The B<super>
privilege is needed if you wish to kill queries in mysql 4.0.2 versions and
above.  In 3.x, the B<process> privilege allows a user to terminate a query.

=back

Initially, B<mtop> does not connect to a specific database.  Most commands this 
program issues are non-database specific (SHOW FULL PROCESSLIST, SHOW VARIABLES, 
KILL id).  However, when database-specific commands are needed, B<mtop> will try to 
connect to the the required database and prompt for a username/password if the default one fails.

To install B<mtop>, run the following shell commands:

    perl Makefile.PL
    make
    make install

The default {install_prefix} is /usr/local which means that B<mtop> is installed 
in /usr/local/bin/.  To change this, run:

    perl Makefile.PL --prefix={install_prefix}
    
or modify the PREFIX line in Makefile.PL.

Requires the following perl modules:

    Module        Available At
    ------------  --------------------------------------------------------
    Curses        http://www.cpan.org/authors/id/WPS
    DBI           Distributed as Bundle::DBI: http://www.cpan.org/authors/id/TIMB
    DBD::mysql    http://www.cpan.org/authors/id/JWIED
    Getopt::Long  (Distributed with Perl 5)
    Net::Domain   Part of libnet: http://www.cpan.org/authors/id/GBARR/

=head1 AUTHOR

Marc Prewitt, Chelsea Networks <mprewitt@chelsea.net>

Copyright (C) 2002 Marc Prewitt/Chelsea Networks, under the GNU GPL.
mtop comes with ABSOLUTELY NO WARRANTY. This is free software, and you are
welcome to redistribute it under certain conditions; see the COPYING file 
for details.

=head1 KNOWN BUGS

The cache hit ratio may look very low if your server receives more than 
4 billion key_read_requests.  This is due to a roll-over condition.  If that
number exceeds 4 billion and change, it rolls over to 1.  The cache 
hit ratio is calculated as: 100 - (key_reads/key_read_requests) * 100.

If you are really trying to focus on the cache hit ratio, flush the status
variables with the B<mtop> B<f> command to reset both the key_reads and
key_read_requests.

Win2K telnet.exe - If you are using the Windows 2000 telnet program, it defaults
to ansi mode which doesn't work well with curses (in my testing on Solaris 8).  
To work around this, set the terminal type to vt100.  To do this, issue the
following command from the telnet prompt before connecting to a host:

    set term vt100

Alternatively, you can manually set your TERM environment variable to vt100
after you are logged in.

=head1 TODO

Offer sorts by other columns

For the 'More:' paging, it would be nice to support 'Less' behaviour.

Add 'n' command and properly calculate number of lines on screen.

    $Id: mtop.PL,v 1.61 2004/09/12 22:22:03 mdprewitt Exp $

=cut

use strict;

use vars qw( $VERSION $RELEASE %PROC_COLS @PROC_COLS %EXPLAIN_COLS @EXPLAIN_COLS $WINDOW_RESIZE );

use DBI;
use DBD::mysql;
use Getopt::Long;
use Curses;
use Net::Domain qw( hostdomain );

my $DOMAIN = hostdomain();

use constant NOECHO => 1;
use constant DESC => 1;

my $opt_host = "";
my $opt_dbport = "";
my $opt_dbsocket = "";
my $opt_dbuser = "mysqltop";
my $opt_passwd = "";
my $opt_seconds = 5;
my $opt_idle = 0;
my $opt_user;
my $opt_version;
my $opt_help;
my $opt_manualrefresh;

my $opt_slow;
my $opt_veryslow;
my $opt_veryveryslow;

my $opt_filterhost;
my $opt_filteruser;
my $opt_filterdb;
my $opt_filtercommand;
my $opt_filterstate;
my $opt_filterinfo;

my $opt_fold;

my $DEBUG;

my $PROMPT_ROW = 3;
my $SORT = DESC;

@PROC_COLS = ( qw( Id User Host db Time Command State Info ) );  # order of labels

#  width of columns, -1 means rest of the screen
%PROC_COLS = (  
        Id=>8,
        User=>8,
        Host=>16,
        db=>12,
        Time=>6,
        Command=>7,
        State=>12,
        Info=>-1 );

@EXPLAIN_COLS = ( qw( table type possible_keys key ken_len ref rows Extra ) );

%EXPLAIN_COLS = ( 
        table => 12,
        type  => 8,
        possible_keys => 20,
        key => 12,
        ken_len => 8,
        'ref' => 8,
        rows => 6,
        Extra => -1
);

my %PROCS;       # place to save proc info between refreshes

my %STATS;       # 'show status' info
my %PREV_STATS;  # info from previous 'show status'

my %VARS;        # 'show variables' info

# dbh's to other databases format is 
# $DB{db_name}
#
my %DB;    

init_screen();

GetFileToArgv(".mtoprc")                if -e ".mtoprc";
GetFileToArgv("$ENV{HOME}/.mtoprc")     if $ENV{HOME} && -e "$ENV{HOME}/.mtoprc";
GetFileToArgv("/usr/local/etc/mtoprc")  if -e "/usr/local/etc/mtoprc";
GetFileToArgv("/etc/mtoprc")            if -e "/etc/mtoprc";

GetOptions(
        'debug:i'               => \$DEBUG,
        'h|host:s'              => \$opt_host,
        'dbuser:s'              => \$opt_dbuser,
        'dbport:i'              => \$opt_dbport,
        'dbsocket:s'            => \$opt_dbsocket,
        'user:s'                => \$opt_user,
        'password:s'            => \$opt_passwd,
        'seconds:i'             => \$opt_seconds,
        'slow:i'                => \$opt_slow,
        'vs|veryslow:i'         => \$opt_veryslow,
        'vvs|veryveryslow:i'    => \$opt_veryveryslow,
        'fu|filter-user:s'      => \$opt_filteruser,
        'fh|filter-host:s'      => \$opt_filterhost,
        'fd|filter-db:s'        => \$opt_filterdb,
        'fc|filter-command:s'   => \$opt_filtercommand,
        'fs|filter-state:s'     => \$opt_filterstate,
        'fi|filter-info:s'      => \$opt_filterinfo,
        'fold-select-columns!'  => \$opt_fold,
        'version'               => \$opt_version,
        'help|?'                => \$opt_help,
        'manualrefresh'         => \$opt_manualrefresh,
        'idle!'                 => \$opt_idle,
    ) || usage();
    
$DEBUG = 1 if defined $DEBUG;

$| = 1;

show_version() if $opt_version;
usage() if $opt_help;

my $dbh;
my %dsn_opts;
$dsn_opts{host}         = "$opt_host"     if $opt_host;
$dsn_opts{port}         = "$opt_dbport"   if $opt_dbport;
$dsn_opts{mysql_socket} = "$opt_dbsocket" if $opt_dbsocket;

$dbh = get_dbh(undef, %dsn_opts) ||
    die "Unable to connect to mysql [", $DBI::errstr, "]\n";

my $st_procs   = $dbh->prepare("show full processlist");
my $st_status  = $dbh->prepare("show status");
my $st_kill    = $dbh->prepare("kill ?");
my $st_flush   = $dbh->prepare("flush status");
my $st_vars    = $dbh->prepare("show variables");
my $st_master_stat = $dbh->prepare("show master status");
my $st_slave_stat = $dbh->prepare("show slave status");
my $st_slave_start = $dbh->prepare("slave start");
my $st_slave_stop = $dbh->prepare("slave stop");

my $CURSES_INIT;
my $SCREEN_WIDTH;
my $SCREEN_HEIGHT;
my $ALMOST_SLOW_COLOR;
my $SLOW_COLOR;
my $REALLY_SLOW_COLOR;

$WINDOW_RESIZE = 0;
$SIG{WINCH} = sub { $WINDOW_RESIZE = 1 };

my $reset = time + $opt_seconds;
refresh_vars();
refresh_screen();

$opt_slow          = $VARS{long_query_time}  unless $opt_slow;
$opt_veryslow      = $opt_slow * 2           unless $opt_veryslow;
$opt_veryveryslow  = $opt_slow * 4           unless $opt_veryveryslow;

while (1) {
    my $key;
    if ( $opt_manualrefresh && scalar %PROCS ) {
        $key = pause("Press any key to continue or e/z:");
    } else {
        $key = getch_nb();
    }
    my $refresh = 0;
    if ($key eq "q") {
        last;
    } elsif ($key eq "k") {
        $refresh = do_kill();
    } elsif ($key eq "s") {
        $refresh = do_seconds();
    } elsif ($key eq "m") {
        if ($opt_manualrefresh) { $opt_manualrefresh = 0 } else { $opt_manualrefresh = 1 };
        next; # skip the rest of the loop to immediately go into manual mode
    } elsif ($key eq "i") {
        if ($opt_idle) { $opt_idle = 0 } else { $opt_idle = 1 };
        $refresh = 1;
    } elsif ($key eq "d") {
        $refresh = do_filter();
    } elsif ($key eq "h") {
        $refresh = do_host();
    } elsif ($key eq "u") {
        $refresh = do_user();
    } elsif ($key eq "+") {
        $refresh = $opt_user = "";
    } elsif ($key eq "e") {
        $refresh = do_explain();
    } elsif ($key eq "T") {
        $refresh = do_admin_stats();
    } elsif ($key eq "t") {
        $refresh = do_stats();
    } elsif ($key eq "v") {
        $refresh = do_vars();
    } elsif ($key eq "F") {
        $opt_fold = !$opt_fold;
    } elsif ($key eq "f") {
        $refresh = do_flush();
    } elsif ($key eq "o") {
        if ($SORT) { $SORT = 0 } else { $SORT = 1 };
        $refresh = 1;
    } elsif ($key eq "z") {
        $refresh = do_zoom();
    } elsif ($key eq "r") {
        $refresh = do_replication();
    } elsif ($key eq "?") {
        $refresh = do_help();
    } else {
        refresh_screen();
    }
    refresh_screen() if $refresh;

    sleep_or_key($opt_seconds);

    if ($WINDOW_RESIZE) {
        handle_resize();
    }
}

#
#  Show detail for a number of commands
#
sub do_zoom {
    my $line = get_string("Id(s)");

    my @ids = split(/\s+/, $line);
    my @info;
    foreach my $id (@ids) {
        push @info,  get_id_detail($id), " ";
    }
    return error("No Ids") unless @info;
    if (scalar @info) {
        more(@info);
        my $ch = pause( "Press any key to continue or 'e' to explain a statement:");
        if (lc($ch) eq 'e') {
            do_explain();
        }
    }
    return 1;
}

#
#  Show the mysqld variables
#
sub do_vars {
    refresh_vars();

    my @keys = sort keys %VARS;
    my $mid = int(scalar @keys / 2);

    my @vars;
    for (my $i=0; $i<=$mid; $i++) {
        my $val1 = $VARS{$keys[$i]};
        my $val2 = $VARS{$keys[$i+$mid]};
        $val1 = commify($val1) if $val1 =~ /^\d+$/;
        $val2 = commify($val2) if $val2 =~ /^\d+$/;
        my $spaces1 = " " x ($SCREEN_WIDTH/2 - length($val1) - length($keys[$i]) - 4);
        my $spaces2 = " " x ($SCREEN_WIDTH/2 - length($val2) - length($keys[$i+$mid]) - 4);
        my $line = "$keys[$i]: $spaces1$val1";
        $line .= "  $keys[$i+$mid]: $spaces2$val2" if $keys[$i+$mid];
        push @vars, $line;
    }
    more(@vars);
    pause();
    return 1;
}

#
#  Show the mysqld status settings
#
sub do_stats {
    my @keys = sort keys %STATS;
    my $mid = int(scalar @keys / 2);

    move(0,0);
    clrtobot();

    do {
        if ($WINDOW_RESIZE) {
            handle_resize();
        }

        my @stats;
        refresh_stats();
        my $i;
        for ($i=0; $i<=$mid; $i++) {
            my $val1 = $STATS{$keys[$i]};
            my $val2 = $STATS{$keys[$i+$mid]};
            $val1 = commify($val1) if $val1 =~ /^\d+$/;
            $val2 = commify($val2) if $val2 =~ /^\d+$/;
            $val1 .= " [" .  int($STATS{$keys[$i]}/ $STATS{Uptime});
            $val2 .= " [" .  int($STATS{$keys[$i+$mid]}/ $STATS{Uptime});

            $val1 .= "/" .  int(($STATS{$keys[$i]}-$PREV_STATS{$keys[$i]}) / $opt_seconds) . "]";
            $val2 .= "/" .  int(($STATS{$keys[$i+$mid]}-$PREV_STATS{$keys[$i+$mid]}) / $opt_seconds) . "]";
            my $spaces1 = " " x ($SCREEN_WIDTH/2 - length($val1) - length($keys[$i]) - 4);
            my $spaces2 = " " x ($SCREEN_WIDTH/2 - length($val2) - length($keys[$i+$mid]) - 4);
            my $line = "$keys[$i]: $spaces1$val1";
            $line .= "  $keys[$i+$mid]: $spaces2$val2" if $keys[$i+$mid];
            addstr($i, 0, $line) if $line;
        }
        addstr($i, 0, "---");
        message("Press any key to return") 
    } while !sleep_or_key($opt_seconds) || getch_nb() == -1;
    return 1;
}

#
#  Show important stats like:
#   Cache Hit Ratio
#   Created_tmp_tables vs Created_tmp_disk_tables
#
sub do_admin_stats {
    my @keys = sort keys %STATS;
    my $mid = int(scalar @keys / 2);

    move(0,0);
    clrtobot();

    do {
        if ($WINDOW_RESIZE) {
            handle_resize();
        }

        my $chit = _getCacheHit(\%STATS);
        my $queries = $STATS{Questions};
        more(
                "Recommendations below come from various places in the MySQL manual.",
                " ",
                "Cache Hit Ratio: " . sprintf("%2.2f%", $chit),
                $chit < 99 ? " (Consider increasing key_buffer_size until cache ratio > 99%) " : "",
                "Tmp tables converted to disk: " . sprintf("%2.2f%", 
                    100*($STATS{Created_tmp_disk_tables}/ ($STATS{Created_tmp_tables} || 1))),
                " (Increase tmp_table_size to decrease this)",
                "Number of joins without keys (Select full joins): " . $STATS{Select_full_join},
                $STATS{Select_full_join} ? " (Look for joins which don't use indexes until SFJ = 0) " : "",
                $STATS{Slow_queries} > 0 ? "Slow queries: $STATS{Slow_queries} (Look in slow query log)" : "",
                #  These next numbers aren't really correct, we really need to know the 
                #  number of reads, not queries.
                "Full index scans: " . sprintf("%2.2f%", $STATS{Handler_read_first} / $queries),
                "Index utilization: " . sprintf("%2.2f%", $STATS{Handler_read_key} / $queries),
                "Range or Table Scans: ". sprintf("%2.2f%", $STATS{Handler_read_next} / $queries),
                "---"
                );

    } while !sleep_or_key($opt_seconds) || getch_nb() == -1;
    return 1;
}

#
#  Show the help screen (clears the screen).
#
sub do_help {
    refresh_vars();
    my $keybuffer = friendly_bytes($VARS{key_buffer_size});
    my $sortbuffer = friendly_bytes($VARS{sort_buffer});
    my $help = qq{mtop ver $VERSION/$RELEASE, Copyright (c) 2002, Marc Prewitt/Chelsea Networks

A top users display for mysql

These single-character commands are available:

q - quit
? - help; show this text
f - flush status
F - fold/unfold column names in select statement display
k - kill processes; send a kill to a list of ids
s - change the number of seconds to delay between updates
m - toggle manual refresh mode on/off
d - filter display with regular expression (user/host/db/command/state/info)
h - display process for only one host
u - display process for only one user
i - toggle all/non-Sleeping process display
o - reverse the sort order
e - explain a process; show query optimizer info
t - show mysqld stats (show status/mysqladmin ext)
T - show mysqld important stats
v - show mysqld variables (show variables/mysqladmin vars)
z - zoom in on a process, show sql statement detail 
r - show replication status for master/slaves

Stats Explanation (See SHOW STATUS docs for full details):

Cache Hit:     Key_read / Key_read_requests. If small, consider increasing 
               key_buffer_size (current=$keybuffer)
Opened tables: If large, consider increasing table_cache 
               (current=$VARS{table_cache})
RRN:           Handler_read_rnd_next High if you are doing a lot of table scans.
TLW:           Table_locks_waited If high, consider optimising queries or 
               splitting db.
SFJ:           Select_full_join Number of joins without keys (Should be 0). 
SMP:           Sort_merge_passes If high, consider increasing sort_buffer 
               (current=$sortbuffer).
    };
    more($help);
    pause();
    return 1;
}

#
#  Change the current users displayed
#
sub do_user {
    $opt_user = get_string("user");
    message("");
    return 1;
}

#
#  Change the current host displayed
#
sub do_host {
    $opt_filterhost = get_string("host");
    message("");
    return 1;
}

#
#  Change the current set of filters used for display
#
sub do_filter {
    my $filteron = lc(get_string("filter type (u/h/d/c/s/i/?): "));
    my $prompt = "enter perl regex for filter or return to clear: ";

    if ($filteron eq 'u') {
        $opt_filteruser = get_string($prompt);
    } elsif ($filteron eq 'h') { 
        $opt_filterhost = get_string($prompt);
    } elsif ($filteron eq 'd') {
        $opt_filterdb = get_string($prompt);
    } elsif ($filteron eq 'c') {
        $opt_filtercommand = get_string($prompt);
    } elsif ($filteron eq 's') {
        $opt_filterstate = get_string($prompt);
    } elsif ($filteron eq 'i') {
        $opt_filterinfo = get_string($prompt);
    } elsif ($filteron eq '?') {
        more("Curent display filters: 

    user    =~ m/$opt_filteruser/i
    host    =~ m/$opt_filterhost/i
    db      =~ m/$opt_filterdb/i
    command =~ m/$opt_filtercommand/i
    state   =~ m/$opt_filterstate/i
    info    =~ m/$opt_filterinfo/i
        ");
        pause();
    } else {
        return error("Invalid filter option");
    }
    return 1;
}

#
#  Change the refresh interval
#
sub do_seconds {
    my $secstr = get_string("seconds");
    if (!$secstr || $secstr =~ /\D/) {
        return error("Illegal value!");
    } else {
        $opt_seconds = $secstr;
    }
    return 1;
}

#
#  Do an explain on the queries in question
#
sub do_explain {
    my $line = get_string("explain");
    my @ids = split(/\s+/, $line);
    my @info;
    message("Waiting for locks...");
    foreach my $id (@ids) {
        push @info,  get_id_explain($id), " ";
    }
    if (scalar @info) {
        more(@info);
    } else {
        return error("No Ids");
    }
    pause();
    return 1;
}

#
#  Kill processes, ask the user for ids and kill each one of them
#
sub do_kill {
    my $line = get_string("kill");
    my @pids = split(/\s+/, $line);
    return error("No Ids") unless @pids;
    addstr($PROMPT_ROW, 0, "killing " . join(" ", @pids));
    foreach (@pids) {
        $st_kill->execute($_) if $_;
    }
    message("");
    return 1;
}

#
#  Show replication status for this host.  Show local masters
#  and slaves running and also connect to any connected remote
#  slaves to get their status.
#
#  Allow the user to restart replication on the local slave.
#
sub do_replication {
    my $key;
    while(1) {
        if ($WINDOW_RESIZE) {
            handle_resize();
        }

        my $key = getch_nb();
        if ( $key eq 's' ) {
            skip_slave_until_running();
        } elsif ($key eq 'r') {
            restart_slave();
        } elsif ($key != -1) {
            last;
        }

        my ($master_pos, $master_file, @info) = repl_getmaster();

        push @info, repl_get_local_slave();

        push @info, repl_get_remote_slave($master_pos, $master_file);
        
        more(@info);

        message("Press any key to return [s] to skip error queries, [r] to restart slave");

        sleep_or_key($opt_seconds);
    }

    return 1;
}

#
#  Return an the current master position and array containing 
#  strings of information about the master status.
#
sub repl_getmaster {
    my ($master_pos, $master_file, @info);

    $st_master_stat->execute() or 
        push @info, "Unable to execute SHOW MASTER STATUS [" .  $dbh->errstr() . "]" and 
        return $master_pos, @info;

    while (my $row = $st_master_stat->fetchrow_hashref()) {
        if ($row->{File}) {
            $master_pos = $row->{Position};
            $master_file = $row->{File};
            push @info, "Master:   $row->{File}:$row->{Position}";
            push @info, "  Do DB:  $row->{Binlog_do_db}" if $row->{Binlog_do_db};
            push @info, "  Ignore: $row->{Binlog_ignore_db}" if $row->{Binlog_ignore_db};
        } else {
            push @info, "Master:   Not configured";
        }
        push @info, " ";
    }
    return $master_pos, $master_file, @info;
}

#
#  Return an array of strings with info about the local
#  slaves.
#
sub repl_get_local_slave {
    my @info;

    $st_slave_stat->execute() or 
        push @info, "Unable to execute SHOW SLAVE STATUS [" .  $dbh->errstr() . "]" and
        return @info;

    my $local_slaves = 0;
    while (my $row = $st_slave_stat->fetchrow_hashref()) {
        if ($row->{Master_Host}) {
            push @info, "Local Slaves ", " " unless $local_slaves++;
            push @info, "Slave:    $row->{Log_File}:$row->{Pos} Running: $row->{Slave_Running}";
            push @info, "  Do DB:  $row->{Replicate_do_db}" if $row->{Replicate_do_db};
            push @info, "  Ignore: $row->{Replicate_ignore_db}" if $row->{Replicate_ignore_db};
            push @info, "  Master: $row->{Master_User}\@$row->{Master_Host}:$row->{Master_Port} Retry: $row->{Connect_retry}";
            push @info, "  Last Error: [$row->{Laster_errno}] $row->{Last_error}" if $row->{Last_error};
        }
        push @info, " ";
    }
    return @info;
}

#
#  Stop/start replication
#
sub restart_slave {
    $st_slave_stop->execute();
    $st_slave_start->execute() or return pause("Unable to start slave [" . $dbh->errstr() . "]"), undef;
    return 1;
}

#
#  Reset slave replication by skipping ahead in the binlog.  
#  TODO: This should really look at the query and table structure and
#  figure out if the counter needs to be set to 1 or 2.
#
sub skip_slave {
    my $count = shift;
    $st_slave_stop->execute();
    my $st_skip = $dbh->prepare("set sql_slave_skip_counter=$count");
    $st_skip->execute() or pause("Unable to skip slave [" . $dbh->errstr() . "]"), undef;
    $st_slave_start->execute() or return pause("Unable to start slave [" . $dbh->errstr() . "]"), undef;
    return 1;
}

#
#  Reset slave replication until it runs successfully for 5 seconds
#
sub skip_slave_until_running {
    my $skipped;
    my $error_time = time;
    do {
        $st_slave_stat->execute() or return pause("Unable to execute SHOW SLAVE STATUS [" .  $dbh->errstr() . "]"), undef;

        while (my $row = $st_slave_stat->fetchrow_hashref()) {
            if ($row->{Master_Host} and $row->{Last_error}) {
                return pause("Unable to skip, this version of mysql doesn't support Skip_counter"), 
                    undef unless exists $row->{Skip_counter};
                skip_slave($row->{Skip_counter}) or return undef;
                $skipped++;
                $error_time = time;
            } else {
                sleep_or_key(1) ;
            }
            message("Skipping in progress, press any key to stop: $skipped");
        }
    } while (time - $error_time < 5) and getch_nb() == -1;
    return 1;
}

#
#  Return an array of strings with info about the remote slaves
#  connected to the master.  The slave binlog offset is calculated
#  from the provided $master_pos variable.
#
sub repl_get_remote_slave {
    my $master_pos = shift;
    my $master_file = shift;
    my @info;

    $st_procs->execute() or
        push @info, "Unable to execute SHOW PROCESSLIST trying to find slaves: " . $dbh->errstr() and
        return @info;

    my $local_slaves = 0;
    while (my $masterrow = $st_procs->fetchrow_hashref()) {
        if ($masterrow->{Command} eq "Binlog Dump") {
            push @info, "Remote Slaves ", " " unless $local_slaves++;
            my $host = $masterrow->{Host};
            if (my $dbh = get_dbh(undef, %dsn_opts, host=>$host)) {
            my $sth = $dbh->prepare("show slave status");
                $sth->execute() || 
                    push @info, "Unable to connect to $host for slave status" . $dbh->errstr();
                while (my $row = $sth->fetchrow_hashref()) {
                    push @info, "$host: $row->{Log_File}:$row->{Pos} Running: $row->{Slave_Running}";
                    push @info, "  Do DB:  $row->{Replicate_do_db}" if $row->{Replicate_do_db};
                    push @info, "  Ignore: $row->{Replicate_ignore_db}" if $row->{Replicate_ignore_db};
                    push @info, "  Master: $row->{Master_User}\@$row->{Master_Host}:$row->{Master_Port} Retry: $row->{Connect_retry}";
                    push @info, "  Last Error: [$row->{Laster_errno}] $row->{Last_error}" if $row->{Last_error};
                    my $offset;
                    if ($row->{Log_File} eq $master_file) {
                        $offset = $master_pos - $row->{Pos};
                        # Don't show negative offset, we checked the master before checking
                        # the slave so we might get a later slave position.
                        $offset = 0 if $offset < 0;  
                    } else {
                        $offset = "> $master_pos";
                    }
                    push @info, "  Offset from Master: " . $offset;
                    push @info, " ";
                }
            } else {
                push @info, "Unable to connect to $host for slave status";
            }
        }
    }
    push @info, "No Remote Slaves Found", " " unless $local_slaves;

    return @info;
}

#
#  Flush the status to reset the global variables
#
sub do_flush {
    $st_flush->execute() || error("Unable to execute flush status " . $dbh->errstr());
    refresh_vars();
}

# 
#  Does a 'show variables' and updates %VARS with the results
#
sub refresh_vars {
    $st_vars->execute() || die "Unable to execute show variables" . $dbh->errstr() . "\n";
    while (my $row = $st_vars->fetchrow_hashref()) {
        $VARS{$row->{Variable_name}} = $row->{Value};
    }
}

# 
#  Does a 'show status' and updates %STATS with the results
#  Saves previous run in %PREV_STATS.  
#
sub refresh_stats {
    $st_status->execute() || die "Unable to execute show status" . $dbh->errstr() . "\n";
    while (my $row = $st_status->fetchrow_hashref()) {
        $PREV_STATS{$row->{Variable_name}} = $STATS{$row->{Variable_name}};
        $STATS{$row->{Variable_name}} = $row->{Value};
    }
}

#
#  Display the header at the top of the page.
#
sub header() {
    refresh_stats();

    my $load_avg = "$opt_host ";
    # Only show load average and memory if we're monitoring the local machine
    if (!$opt_host) {
        $load_avg .= `uptime`;
        chomp($load_avg);
        $load_avg =~ s/.*load/load/;

    }
    my $mysql_memory = get_mysql_memory();
    $mysql_memory = "  MEM: " . friendly_bytes($mysql_memory) if $mysql_memory;

    my $time  = $STATS{Uptime}; $time =~ s/,//g;
    my $days  = int( $time / 86400);
    my $hours = int(($time % 86400) / (60 * 60));
    my $min   = int(($time % 86400) / 60 % 60);
    $min   = "0$min"   if $min < 10;
    $hours = " $hours" if $hours < 10;
    
    my $chit = sprintf("%2.2f", _getCacheHit(\%STATS));

    my $qps         = friendly_number(($STATS{Questions} - $PREV_STATS{Questions})/$opt_seconds);
    my $questions   = friendly_number($STATS{Questions}, 1);
    my $slow        = friendly_number($STATS{Slow_queries}, 1);
    my $open_tables = friendly_number($STATS{Opened_tables}, 1);
    my $rrn         = friendly_number($STATS{Handler_read_rnd_next}, 1);
    my $tlw         = friendly_number($STATS{Table_locks_waited}, 1);
    my $sfj         = friendly_number($STATS{Select_full_join}, 1);
    my $smp         = friendly_number($STATS{Sort_merge_passes}, 1);

    addstr(0,0, $load_avg . " mysqld $VARS{version} up $days day(s), $hours:$min hrs");
    addstr(1,0, "$STATS{Threads_connected} threads: $STATS{Threads_running} running, $STATS{Threads_cached} cached. Queries/slow: $questions/$slow Cache Hit: $chit%");

    addstr(2,0, "Opened tables: $open_tables  RRN: $rrn  TLW: $tlw  SFJ: $sfj  SMP: $smp  QPS: $qps$mysql_memory");
    if ($opt_fold) {
        standout();
        addstr(0,$SCREEN_WIDTH-2, "F");
        standend();
    } else {
        addstr(0,$SCREEN_WIDTH-2, " ");
    }
    
    if ($opt_manualrefresh) {
        standout();
        addstr(0,$SCREEN_WIDTH-1, "M");
        standend();
    } else {
        addstr(0,$SCREEN_WIDTH-1, " ");
    }
}

#
#  Put a new header on the screen.
#  Grab new process list, save it in %PROCS by Id and display.
#
sub refresh_screen {
    move(0, 0);
    clrtobot();
    header();
    $st_procs->execute() || die "Unable to execute show procs [" . $dbh->errstr() . "]\n";
    my @rows;
    while (my $row = $st_procs->fetchrow_hashref()) {
        if ( filter_ok($row) ) {
            push @rows, $row;
        }
    }
    my $rownum = 4;
    my $c = 0;
    foreach my $col (@PROC_COLS) {
        addstr($rownum, $c, uc($col));
        $c += $PROC_COLS{$col} +1;
    }
    $rownum++;
    %PROCS = ();
    foreach my $row (sort sort_procs @rows) {
        $PROCS{$row->{Id}} = $row;
        $c = 0;
        foreach my $col (@PROC_COLS) {
            my $width = $PROC_COLS{$col};
            if ( $width == -1 ) {
                $width = $SCREEN_WIDTH - $c;
            }

            my $data = $row->{$col};
            # Remove nl and multi spaces so that data doesn't move off the 
            # line it's supposed to be on.
            $data =~ s/\n//g;
            $data =~ s/\.$DOMAIN//o if $col eq "Host";
            if ($col eq "Info") {
                $data = strip_comments($data);
                $data = select_fold($data);
            }
            $data =~ s/\s+/ /g;
            $data =~ s/^\s+//;
            $data = substr($data, 0, $width);  # limit the data to the width of the column

            my $query_color = query_color($row);
            attron($query_color) if $query_color;
            addstr($rownum, $c, $data) if $data;
            attroff($query_color) if $query_color;
            $c += $width +1;
        }
        $rownum++;
    }
    addstr($rownum, 0, "---");
    move(0,0);
    refresh();
}

#
#  Returns an array consisting of detail for a command
#
sub get_id_detail {
    my $id = shift;
    
    if ($id =~ /\D/) {
        return "Id: $id is an invalid id number.";
    } elsif (!exists $PROCS{$id}) {
        return "Id: $id not found";
    }
    my $proc = $PROCS{$id};
    return (
            "Id: $id User: $proc->{User} Host: $proc->{Host} Db: $proc->{db} Time: $proc->{Time}",
            "Command: $proc->{Command} State: $proc->{State}", " ",
            split_sql($proc->{Info}), ""
    );
}

#
#  Returns an array consisting of explain info for a query
#
sub get_id_explain {
    my $id = shift;
    
    if ($id =~ /\D/) {
        return "Id: $id is an invalid id number.";
    } elsif (!exists $PROCS{$id}) {
        return "Id: $id not found";
    }

    my $db = $PROCS{$id}->{db};
    my $dbh = get_dbh($db, %dsn_opts) || return;

    my $st_explain = $dbh->prepare("explain $PROCS{$id}->{Info}");
    $st_explain->execute();

    my @rows = (get_id_detail($id), " ");
    push @rows, sprintf("%-12.12s|%-8.8s|%-20.20s|%-12.12s|%8.8s|%-8.8s|%6.6s|", @EXPLAIN_COLS);
    while (my $row = $st_explain->fetchrow_arrayref()) {
        if ($row) {
            my $line = sprintf("%-12.12s|%-8.8s|%-20.20s|%-12.12s|%8.8s|%-8.8s|%6.6s", @$row);
            my $leftover = $SCREEN_WIDTH - length($line) - 1;
            $line .= "|" . substr($row->[-1], 0, $leftover);
            push @rows, $line;
        }
    }
    return @rows;
}

############################################################################
#  
#  Utility routines
#

sub commify {
    local $_  = shift;
    return 0 unless defined $_;
    1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
    return $_;
}

#
#  Prompt the user for input on the PROMPT_ROW and return what they typed in
#
sub get_string {
    my $prompt = shift;
    my $noecho = shift;
    my $string = "";

    move($SCREEN_HEIGHT-1, 0);
    clrtoeol();
    standout();
    addstr($SCREEN_HEIGHT-1, 0, $prompt);
    standend();
    move($SCREEN_HEIGHT-1, length($prompt) + 1);
    if ($noecho) { noecho() } else { echo() }
    nodelay(0);  # blocking-reads
    getnstr($string, 1024);  # prevent possible buffer overflow with getn
    move($SCREEN_HEIGHT-1, 0);
    clrtoeol();
    chomp($string);
    return $string;
}

#
#  Display info on the screen on 'page' at a time.
#  Strings are split on '\n'.
#
sub more {
    move(0,0);
    clrtobot();
    my $row = 0;
    foreach (@_) {
        foreach (split(/\n/)) {
            addstr($row++, 0, $_);
            if ($row == $SCREEN_HEIGHT-1) {
                return if lc(pause("More: ")) eq "q";
                move(0,0);
                clrtobot();
                $row = 0;
            }
        }
    }
    refresh();
}

#
#  Displays the first argument or 'press any key' at the bottom of the 
#  screen and waits for the user to press a key.  The message is cleared
#  after a key is pressed.
#
#  Returns the character that was pressed.
#
sub pause {
    my $msg = shift || "Press any key to continue:";
    standout();
    addstr($SCREEN_HEIGHT-1, 0, $msg);
    standend();
    refresh();
    nodelay(0);             # so getch() is blocking
    my $ch = getch();
    move($SCREEN_HEIGHT-1, 0);
    clrtoeol();
    refresh();
    return $ch;
}

#
# Print a highlighted message at the bottom of the screen
#
sub message {
    my $msg = shift;
    standout();
    move($SCREEN_HEIGHT-1, 0);
    clrtoeol();
    addstr($SCREEN_HEIGHT-1, 0, $msg);
    standend();
    move(0,0);
    refresh();
    return 1;
}

#
#  call &message() and return 0
#
sub error {
    message(@_);
    return 0;
}

# 
#  Sleep for the number of seconds provided or
#  until a key is pressed (on stdin)
#
#  Returns 0 if timed out, 1 if key pressed
#
#  From the 'Perl Cookbook', Chapter 15.12, Managing the Screen
#  and 7.13, Reading from Many File Handles Without Blocking
#
sub sleep_or_key {
    my $secs = shift;

    my ($in, $out) = ('', '');
    vec($in,fileno(STDIN),1) = 1;         # look for key on stdin 
    my $nfound = select($out = $in,undef,undef,$secs); # wait up to this long
    if ($nfound && vec($out, fileno(STDIN), 1)) {
        return 1;
    } else {
        return 0;
    }
}

#
#  Returns a key pressed in non-blocking mode
#  Doesn't restore previous blocking mode, so
#  reset as needed
#
sub getch_nb {
    noecho();               
    nodelay(1);             # so getch() is non-blocking
    return getch();
}

#
#  Split a sql command into multiple lines nicely formatted.
#  An array of lines is returned.
#
sub split_sql {
    my $sql = shift;
    $sql =~ s/,\s*/, /g;
    $sql =~ s/\s+,/,/g;
    $sql =~ s/\(\s+/\(/g;
    $sql =~ s/\s+\)/\)/g;
    $sql =~ s/\s+/ /g;
    $sql =~ s/\s+from\s+/\n  FROM /i;
    $sql =~ s/\s+values\s+/\n  VALUES /i;
    $sql =~ s/\s+into\s+/\n  INTO /i;
    $sql =~ s/\s+where\s+/\n  WHERE /i;
    $sql =~ s/\s+having\s+/\n  HAVING /i;
    $sql =~ s/\s+limit\s+/\n  LIMIT /i;
    $sql =~ s/\s+procedure\s+/\n  PROCEDURE /i;
    $sql =~ s/\s+order\s+by\s+/\n  ORDER BY /i;
    $sql =~ s/\s+group\s+by\s+/\n  GROUP BY /i;
    my @result;
    foreach my $line (split(/\n/, $sql)) {
        while ($line) {
            my $linewidth = length($line);
            if ($linewidth > $SCREEN_WIDTH) {
                $linewidth = rindex($line, " ", $SCREEN_WIDTH);
                if ($linewidth == -1) { $linewidth = $SCREEN_WIDTH; }
            }
            push @result, substr($line, 0, $linewidth);
            $line = substr($line, $linewidth);
            $line = "    $line" if $line;
        }
    }
    return @result;
}

#
#  Convert a number into KB, MB, GB, TB
#
sub friendly_bytes {
    return friendly_number(@_) . "B";
}

#
#  Convert a number into K, M, G, T
#
#  $i: number to convert
#  $d: number of decimal places to show if $i is < 1024
#
sub friendly_number {
    my $i = shift;
    my $d = shift;

    my ($size, $ret_i);
    if ($i < 1024) {
        $ret_i = $i;
    } elsif ( ($i = $i/1024) < 1024 ) {
        $ret_i = $i;
        $size = "K";
    } elsif ( ($i = $i/1024) < 1024 ) {
        $ret_i = $i;
        $size = "M";
    } elsif ( ($i = $i/1024) < 1024 ) {
        $ret_i = $i;
        $size = "G";
    } else {
        $ret_i = $i = $i/1024;
        $size = "T";
    }
    if ($d && int($ret_i) != $ret_i) {
        $ret_i = sprintf("%.${d}f", $ret_i);
    } else {
        $ret_i = int($ret_i);
    }
    return "$ret_i$size";
}

#  
#  Return 1 if the specified row passes the current set of 
#  dipslay filters.
#
sub filter_ok {
    my $row = shift;
    return 1 if
        (!$opt_user          or $row->{User}    eq $opt_user) &&
        (!$opt_filteruser    or $row->{User}    =~ /$opt_filteruser/i) &&
        (!$opt_filterhost    or $row->{Host}    =~ /$opt_filterhost/i) &&
        (!$opt_filterdb      or $row->{Db}      =~ /$opt_filterdb/i) &&
        (!$opt_filtercommand or $row->{Command} =~ /$opt_filtercommand/i) &&
        (!$opt_filterstate   or $row->{State}   =~ /$opt_filterstate/i) &&
        (!$opt_filterinfo    or $row->{Info}    =~ /$opt_filterinfo/i) &&
        ($opt_idle           or $row->{Command} ne "Sleep" );
    return 0;
}

#
#  Remove column names from a select statemetn and replace with ...
#
my @select_mods = qw( ALL DISTINCT DISTINCTROW 
    HIGH_PRIORITY STRAIGHT_JOIN
    SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT
    SQL_CACHE SQL_NO_CACHE SQL_CALC_FOUND_ROWS
);
my $mods = join('|', @select_mods);
sub select_fold {
    my $row = shift;
    
    $row =~ s/(select\s+($mods)?).*(into|from)/$1 ... $3/i;
    return $row;
}

#
#  Return the attribute that should be used to display a query based
#  on how long the query is taking.  Returns 0 if attribute shouldn't 
#  be changed.
#
sub query_color {
    my $sth = shift;
    if ($sth->{Command} eq "Query") {
        my $time = $sth->{Time};
        if ($time > $opt_veryveryslow) {
            return $REALLY_SLOW_COLOR;
        } elsif ($time > $opt_veryslow) {
            return $SLOW_COLOR;
        } elsif ($time > $opt_slow) {
            return $ALMOST_SLOW_COLOR;
        }
    }
    return 0;
}

#
#  Initializes curses and set global screen constants
#  see curs_attr(3CURSES) for details
#
sub init_screen {
    $CURSES_INIT = 1 if initscr();      # start screen
    cbreak(); 
    getmaxyx($SCREEN_HEIGHT, $SCREEN_WIDTH);

    if (has_colors()) {
        start_color();
        init_pair(1, COLOR_RED, COLOR_BLACK);
        init_pair(2, COLOR_YELLOW, COLOR_BLACK);
        init_pair(3, COLOR_MAGENTA, COLOR_BLACK);
        $ALMOST_SLOW_COLOR = COLOR_PAIR(3);
        $SLOW_COLOR = COLOR_PAIR(2);
        $REALLY_SLOW_COLOR = COLOR_PAIR(1);
    } else {
        $ALMOST_SLOW_COLOR = A_DIM;
        $SLOW_COLOR = A_BOLD;
        $REALLY_SLOW_COLOR = A_REVERSE;
    }
}
#
#  Reinitialize curses system.
#
sub handle_resize {
    cleanup_win();
    init_screen();
    refresh();
    $WINDOW_RESIZE = 0;
}

sub sort_procs {
    if ($SORT == DESC) {
        return $b->{Time} <=> $a->{Time};
    } else {
        return $a->{Time} <=> $b->{Time};
    }
}

sub show_version {
    cleanup_win();
    print "\n", version(), "\n";
    exit;
}

#
#  Returns the pid of the running mysql server or
#  0 if the pid cannot be determined.
#
sub get_mysql_pid {
    if ( -r $VARS{pid_file} ) {
        open(PID, $VARS{pid_file}) || return undef;
        chomp(my $pid = <PID>);
        return $pid;
    } else {
        return 0;
    }
}

#
#  Returns the amount of memory used by the server.
#  Currently only works if the server is the local machine.
#
my $MYSQL_PID;
sub get_mysql_memory {
    if (!$opt_host) {
        $MYSQL_PID = get_mysql_pid() unless defined $MYSQL_PID;

        if ( $MYSQL_PID && -f "/proc/$MYSQL_PID/as" ) {
            return ((stat(_))[7]) 
        }
    }
    return undef;
}

#
#  Remove SQL comments.
#  Code from: http://aspn.activestate.com/ASPN/Cookbook/Rx/Recipe/59811
#  Author unattributed on activestate but it looks like it's based on 
#  code in "Mastering Regular Expressions" by Jeffrey E F Friedl
#
sub strip_comments {
    my $str = shift;
    # Build the comment regex up step by step.
    # 1. A complicated regex which matches C-style comments.
    my $regex_comment = qr{/\*[^*]*\*+([^/*][^*]*\*+)*/};

    # 2. A regex which matches double-quoted strings.
    my $regex_double = qr{(?:\"(?:\\.|[^\"\\])*\")};

    # 3. A regex which matches single-quoted strings.
    my $regex_single = qr{(?:\'(?:\\.|[^\'\\])*\')};

    # 4. Now combine 1 through 3 to produce a regex which
    #    matches _either_ double or single quoted strings
    #    OR comments. 
    $str =~ s{($regex_double|$regex_single)|($regex_comment)}{$1}g;
    return $str;
}

#
#  Returns a dbh for a mysql db.  Prompts the user
#  for a username/password if unable to connect as the default
#  user.
#
#  Additional params will be appended to the dsn.
# 
#  dbh's are cached so that the next time the same one is needed
#  a reconnection is not needed.
#
sub get_dbh {
    my $db = shift;
    my %params = @_;

    my $dsn_opts = get_dsnoptstr(%params);

    my $dbh;
    my $dsn = "DBI:mysql:$db$dsn_opts";
    if (!($dbh = $DB{$dsn})) { 

        # Try to connect as the default user
        print STDERR "Getting dbh for $dsn with user=$opt_dbuser\n" if $DEBUG;
        $dbh = DBI->connect($dsn, $opt_dbuser, $opt_passwd, {PrintError=>0});
        if (!$dbh) {
            # Otherwise, prompt for an alternate username/password
            my $dbname = $dsn;
            $dbname =~ s/.*://;
            my $alt_dbuser = get_string("Unable to connect to $dbname as $opt_dbuser, enter another user user: ");
            my $alt_passwd = get_string("Password: ", NOECHO);
            $dbh = DBI->connect($dsn, $alt_dbuser, $alt_passwd, {PrintError=>0}) || 
                pause("Unable to connect to $dsn as $opt_dbuser or $alt_dbuser") && return 0;
            print STDERR "Getting dbh for $dsn with user=$alt_dbuser\n" if $DEBUG;
        }
        $DB{$dsn} = $dbh;
    }
    return $dbh;
}

#
#  Return a list of dsn options specified by key/value pairs in %params.
#  Return format is:
#     ;key1=val1;key2=val2;...
#
sub get_dsnoptstr {
    my %params = @_;
    my $ret;
    foreach my $key (keys %params) {
        $ret .= ";$key=$params{$key}";
    }
    return $ret;
}

sub version {
    return "mtop ver $VERSION/$RELEASE";
}

sub cleanup_win {
    if ($CURSES_INIT) {
        # the move/clear/refresh looks like it should work,
        # however, it looks list it causes stderr to be cleared
        # after endwin
        # move(0,0);
        # clrtobot();
        # refresh();
        endwin();
    }
}

sub GetFileToArgv {
    my $file = shift;
    open(FL, $file) || return error("Unable to open '$file' [$!]");
    while (<FL>) {
        chomp();
        # stript comments and leading blanks
        s/#.*//;
        s/^\s+//;

        next if /^\s*$/;  # ignore blank lines
        unshift @ARGV, split(/\s+/);
    }
}

#
#  If we're in the debugger, don't do this!
#

BEGIN {
    unless ( defined $DB::VERSION ) {
        sub in_eval {
            my $i = 0;
            while (my $sub = (caller($i++))[3]) {
                return 1 if $sub eq "(eval)";
            }
            return;
        }
        my $Die = sub {
            if (in_eval()) {
                #
                #  We are in an eval, so wake up and die right.
                #
                CORE::die @_;
            } else {
                cleanup_win();
                print STDERR @_;
                print STDERR "Stack Trace: \n" if caller(2);
                print STDERR _getStackTrace();
            }
        };

        my $current;
        if ( $current = $SIG{__DIE__} and ref($current) eq "CODE") {
            $SIG{__DIE__} = sub { &$Die(@_); &$current(@_); }
        } else {
            $SIG{__DIE__} = sub { &$Die(@_); CORE::die "\n"; }
        }
    }
}

sub _getStackTrace {
    my $calling_sub = "main";
    my $trace;
    my $i = 2;
    while ( my @level = caller($i++) ) {
        my $line = "    at $calling_sub($level[1]:$level[2])";
        if ($level[5]) {
            $line .= " in array context";
        }
        $trace = "$line\n$trace";
        $calling_sub = $level[3];
    }
    return $trace;
}

sub _getCacheHit {
    my $stats = shift;

    my $krr = $stats->{Key_read_requests} || 1;
    my $kr  = $stats->{Key_reads};
    return 100 - ($kr/$krr) * 100;
}

END {
    cleanup_win();
}

sub usage {
    cleanup_win();
    print "\n", version(), qq{

Copyright (C) 2002 Marc Prewitt/Chelsea Networks <mprewitt\@chelsea.net>
mtop comes with ABSOLUTELY NO WARRANTY. This is free software, and you are
welcome to redistribute it under certain conditions; see the COPYING file 
for details.

Usage: mtop [OPTIONS]

  --version                  Show version number and exit
  --help                     Show this screen and exit
  --host={mysql_host}        Connect to the MySQL server on {mysql_host}
  --dbuser={mysql_user}      Connect to the MySQL server as {mysql_user}
  --password={mysqluser_pw}  Use {mysqluser_pw} when connecting
  --seconds={refresh}        Refresh the screen each {refresh} seconds
  --[no]idle                 Display/don't display idle threads
  --filter-user={regex}      Filter display based on user regular expression
  --filter-host={regex}      Filter display based on host regular expression
  --filter-db={regex}        Filter display based on db regular expression
  --filter-command={regex}   Filter display based on command regular expression
  --filter-state={regex}     Filter display based on state regular expression
  --filter-info={regex}      Filter display based on info regular expression
  --user={user}              Display threads for only {user}
  --manualrefresh            Wait for user input between refreshes

All options can be truncated to their shortest unique abbreviation.

See 'man mtop' or 'perldoc mtop' for more information.

};
    exit();
}


=begin showsatus

show status;

+--------------------------+------------+
| Variable_name            | Value      |
+--------------------------+------------+
| Aborted_clients          | 494        |
| Aborted_connects         | 0          |
| Bytes_received           | 1875816718 |
| Bytes_sent               | 1474745403 |
| Connections              | 3620       |
| Created_tmp_disk_tables  | 1          |
| Created_tmp_tables       | 147386     |
| Created_tmp_files        | 0          |
| Delayed_insert_threads   | 0          |
| Delayed_writes           | 0          |
| Delayed_errors           | 0          |
| Flush_commands           | 1          |
| Handler_delete           | 1133857    |
| Handler_read_first       | 34264      |
| Handler_read_key         | 39609950   |
| Handler_read_next        | 45171610   |
| Handler_read_prev        | 669        |
| Handler_read_rnd         | 98270      |
| Handler_read_rnd_next    | 34320339   |
| Handler_update           | 1317202    |
| Handler_write            | 3900317    |
| Key_blocks_used          | 62108      |
| Key_read_requests        | 1588523835 |
| Key_reads                | 16475545   |
| Key_write_requests       | 24619937   |
| Key_writes               | 451486     |
| Max_used_connections     | 39         |
| Not_flushed_key_blocks   | 32985      |
| Not_flushed_delayed_rows | 0          |
| Open_tables              | 224        |
| Open_files               | 449        |
| Open_streams             | 0          |
| Opened_tables            | 7081       |
| Questions                | 5894332    |
| Select_full_join         | 0          |
| Select_full_range_join   | 4          |
| Select_range             | 250520     |
| Select_range_check       | 0          |
| Select_scan              | 17094      |
| Slave_running            | ON         |
| Slave_open_temp_tables   | 0          |
| Slow_launch_threads      | 0          |
| Slow_queries             | 773        |
| Sort_merge_passes        | 0          |
| Sort_range               | 27         |
| Sort_rows                | 189581     |
| Sort_scan                | 407        |
| Table_locks_immediate    | 6006913    |
| Table_locks_waited       | 4          |
| Threads_cached           | 0          |
| Threads_created          | 3617       |
| Threads_connected        | 19         |
| Threads_running          | 1          |
| Uptime                   | 599379     |
+--------------------------+------------+

show variables;

+---------------------------------+-------------------------------------------------------------+
| Variable_name                   | Value                                                       |
+---------------------------------+-------------------------------------------------------------+
| back_log                        | 50                                                          |
| basedir                         | /opt/mysql/3.23.46/                                         |
| bdb_cache_size                  | 8388600                                                     |
| bdb_log_buffer_size             | 131072                                                      |
| bdb_home                        | /export/DB/mysqldb/                                         |
| bdb_max_lock                    | 10000                                                       |
| bdb_logdir                      |                                                             |
| bdb_shared_data                 | OFF                                                         |
| bdb_tmpdir                      | /tmp/                                                       |
| bdb_version                     | Sleepycat Software: Berkeley DB 3.2.9a: (November 28, 2001) |
| binlog_cache_size               | 32768                                                       |
| character_set                   | latin1                                                      |
| character_sets                  | latin1 cp1251                                               |
| concurrent_insert               | ON                                                          |
| connect_timeout                 | 5                                                           |
| datadir                         | /export/DB/mysqldb/                                         |
| delay_key_write                 | ON                                                          |
| delayed_insert_limit            | 100                                                         |
| delayed_insert_timeout          | 300                                                         |
| delayed_queue_size              | 1000                                                        |
| flush                           | OFF                                                         |
| flush_time                      | 0                                                           |
| have_bdb                        | YES                                                         |
| have_gemini                     | NO                                                          |
| have_innodb                     | NO                                                          |
| have_isam                       | YES                                                         |
| have_raid                       | NO                                                          |
| have_openssl                    | NO                                                          |
| have_symlink                    | YES                                                         |
| init_file                       |                                                             |
| interactive_timeout             | 28800                                                       |
| join_buffer_size                | 131072                                                      |
| key_buffer_size                 | 67104768                                                    |
| language                        | /opt/mysql/3.23.46/share/mysql/english/                     |
| large_files_support             | ON                                                          |
| locked_in_memory                | OFF                                                         |
| log                             | OFF                                                         |
| log_update                      | OFF                                                         |
| log_bin                         | OFF                                                         |
| log_slave_updates               | OFF                                                         |
| log_long_queries                | ON                                                          |
| long_query_time                 | 1                                                           |
| low_priority_updates            | ON                                                          |
| lower_case_table_names          | 0                                                           |
| max_allowed_packet              | 1047552                                                     |
| max_binlog_cache_size           | 4294967295                                                  |
| max_binlog_size                 | 1073741824                                                  |
| max_connections                 | 100                                                         |
| max_connect_errors              | 10                                                          |
| max_delayed_threads             | 20                                                          |
| max_heap_table_size             | 16777216                                                    |
| max_join_size                   | 4294967295                                                  |
| max_sort_length                 | 1024                                                        |
| max_user_connections            | 0                                                           |
| max_tmp_tables                  | 32                                                          |
| max_write_lock_count            | 4294967295                                                  |
| myisam_max_extra_sort_file_size | 256                                                         |
| myisam_max_sort_file_size       | 2047                                                        |
| myisam_recover_options          | 0                                                           |
| myisam_sort_buffer_size         | 8388608                                                     |
| net_buffer_length               | 16384                                                       |
| net_read_timeout                | 30                                                          |
| net_retry_count                 | 10                                                          |
| net_write_timeout               | 60                                                          |
| open_files_limit                | 0                                                           |
| pid_file                        | /export/DB/mysqldb/mysqld.pid                               |
| port                            | 3306                                                        |
| protocol_version                | 10                                                          |
| record_buffer                   | 16773120                                                    |
| record_rnd_buffer               | 16773120                                                    |
| query_buffer_size               | 0                                                           |
| safe_show_database              | OFF                                                         |
| server_id                       | 20                                                          |
| slave_net_timeout               | 3600                                                        |
| skip_locking                    | ON                                                          |
| skip_networking                 | OFF                                                         |
| skip_show_database              | OFF                                                         |
| slow_launch_time                | 2                                                           |
| socket                          | /var/tmp/mysql.sock                                         |
| sort_buffer                     | 16777208                                                    |
| sql_mode                        | 0                                                           |
| table_cache                     | 256                                                         |
| table_type                      | MYISAM                                                      |
| thread_cache_size               | 0                                                           |
| thread_concurrency              | 10                                                          |
| thread_stack                    | 65536                                                       |
| transaction_isolation           | READ-COMMITTED                                              |
| timezone                        | EST                                                         |
| tmp_table_size                  | 8388608                                                     |
| tmpdir                          | /tmp/                                                       |
| version                         | 3.23.46-log                                                 |
| wait_timeout                    | 28800                                                       |
+---------------------------------+-------------------------------------------------------------+

=end

!NO!SUBS!

close OUT or die "Can't close $file: $!";
chmod 0755, $file or die "Can't reset permissions for $file: $!\n";
