#!/bin/sh
echo "Usage: ./scan-fw-range.sh <range>(yesterday|today|nextday|all)"
game=fw
server=$1
quest=$2
script=scan_fw_achieve_uniquest_${quest}.pl
if [[ $quest == '' ]]; then
	echo "must enter quest"
	exit
fi

starttime=`date '+%F %T'`
echo "Starting time:$starttime"
cd /apps/fw/
for uniquest in `ls -1 /data2/gamelogs2/fw/$server/*world.formatlog`
do 
	echo "./$script $uniquest $server"
	./$script $uniquest $server
done

endtime=`date '+%F %T'`
echo "$starttime Start time"
echo "$endtime End time"
exit
