#!/bin/sh
for p in 39 42 44 46 47 48 49; do 
	/usr/bin/perl /apps/fw/scan-fw-daily.pl fw-formatlog-task.pl fw /gamelogs2/$p today formatlog $p
done
