#!/bin/sh
echo "Usage: ./scan-fw-range.sh <range>(yesterday|today|nextday|all)"
game=fw
SRC_LOG=/data2/gamelogs2/fw
masterscript=scan-fw-daily.pl
logscript=/apps/$game/$masterscript
starttime=`date '+%F %T'`
echo "Starting time:$starttime"
for logtype in 'formatlog' 'log' 'trace'
do 
	for zone in 46 47 48 49 
	do
			#echo "$zone and $logtype"
			if [[ $logtype = 'chat' ]]; then
				echo "$zone - $logtype"
				#/usr/bin/perl $logscript scan_${game}_chat-x.pl $game /gamelogs2/$zone $1 chat.gz $zone 
				# achievement
			fi
			if [[ $logtype = 'formatlog' ]]; then
                echo "$zone - $logtype"
				/usr/bin/perl $logscript scan_${game}_roleid_to_user.pl $game $SRC_LOG/$zone $1 formatlog $zone
				echo "`date '+%F %T'` Sleep for 20 second"
#				sleep 20
				echo "`date '+%F %T'` Resume from sleep"
				/usr/bin/perl $logscript scan_${game}_guild.pl $game $SRC_LOG/$zone $1 formatlog $zone
				# achievement
            fi
			if [[ $logtype = 'log' ]]; then
                echo "$zone - $logtype"
				#/usr/bin/perl $logscript scan_${game}_roleid_to_user-log-x.pl $game $SRC_LOG/$zone $1 log.gz $zone
				#/usr/bin/perl $logscript scan_${game}_userlink-trade-log-x.pl $game /gamelogs2/$zone $1 log.gz $zone
				# achievement
            fi
			if [[ $logtype = 'trace' ]]; then
                echo "$zone - $logtype"
				/usr/bin/perl $logscript scan_${game}_name.pl $game $SRC_LOG/$zone $1 trace.gz $zone
#				/usr/bin/perl $logscript scan_${game}_role_del-x.pl $game /gamelogs2/$zone $1 trace.gz $zone
            fi
	done
done

endtime=`date '+%F %T'`
echo "$starttime Start time"
echo "$endtime End time"
exit
