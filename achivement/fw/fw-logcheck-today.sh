#!/bin/sh

for zone in 39 42 44 46 47 48 49
do 
	for logtype in 'formatlog' 'log' 'trace'
	do
#			echo "$zone and $logtype"
#			if [[ $logtype = 'chat' ]]; then
#				echo "$zone - $logtype"
#				/usr/bin/perl /apps/fw/fw-logcheck.pl fw-chat.pl fw /gamelogs2/$zone today chat.gz $zone
#			fi
			if [[ $logtype = 'formatlog' ]]; then
                echo "$zone - $logtype"
				/usr/bin/perl /apps/fw/fw-logcheck.pl fw-formatlog.pl fw /gamelogs2/$zone today formatlog $zone
            fi
			if [[ $logtype = 'log' ]]; then
               echo "$zone - $logtype"
				/usr/bin/perl /apps/fw/fw-logcheck.pl fw-log.pl fw /gamelogs2/$zone today log.gz $zone
            fi
			if [[ $logtype = 'trace' ]]; then
                echo "$zone - $logtype"
				/usr/bin/perl /apps/fw/fw-logcheck.pl fw-trace.pl fw /gamelogs2/$zone today trace.gz $zone
			fi
	done
done
exit
