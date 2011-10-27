#!/usr/bin/env sh

# <editable area
EXE=mtc-rplerr-monitor
LOGFILE_PREFIX=/logs/mysql-rplmon
PIDFILE_PREFIX=/tmp/mysql-rplmon
# slave example: 
#    mysql2:h=172.29.x.x,P=3307,u=rpl,p=xxx
# or simplely: 
#    mysql2
# with everything set to defaults.
SLAVES="\
mysql4 \
mysql7 \
mysql8 \
mysql9 \
mysql10 \
mysql11 \
mysql12 \
mysql13 \
mysql17 \
mysql18 \
mysql19 \
mysql20 \
mysql23 \
mysql24 \
mysql25 \
mysql26 \
mysql27 \
mysql28 \
mysql29 \
mysql30 \
mysql32 \
mysql3-eu \
mysql4-eu \
mysql5-eu \
mysql6-eu \
mysql7-eu \
mysql8-eu \
mysql9-eu \
mysql10-eu \
mysql11-eu \
mysql12-eu \
mysql13-eu \
mysql14-eu \
mysql15-eu \
mysql18-eu \
"
# exitable area>


if [ ! -d "$PIDFILE_PREFIX" ];then
    mkdir -p "$PIDFILE_PREFIX"
    chown mysql:mysql "$PIDFILE_PREFIX"
fi
if [ ! -d "$LOGFILE_PREFIX" ];then
    mkdir -p "$LOGFILE_PREFIX"
    chown mysql:mysql "$LOGFILE_PREFIX"
fi
case "$1" in
    'start')
        for i in $SLAVES;do
            pidfile="$PIDFILE_PREFIX/${i%%:*}.pid"
            log="$LOGFILE_PREFIX/${i%%:*}.log"
            stderr="$LOGFILE_PREFIX/${i%%:*}.stderr"
            nid="${i#*:}"
            if [ "$nid" == "$i" ];then
                                nid="h=$i"
            fi
            start-stop-daemon --start --pidfile="$pidfile" --exec="$EXE" \
                --chdir=/tmp --chuid=mysql:mysql --background --oknodo -- \
                -pidfile="$pidfile" -m sysadmins@perfectworld.com \
                -l="$log" -e="$stderr" "$nid"
        done
        ;;
    'stop')
        for i in $SLAVES;do
            pidfile="$PIDFILE_PREFIX/${i%%:*}.pid"
            errlog="$LOGFILE_PREFIX/${i%%:*}.errlog"
            stderr="$LOGFILE_PREFIX/${i%%:*}.stderr"
            start-stop-daemon --stop --pidfile="$pidfile" --exec="$EXE" \
                --user=mysql --group=mysql --oknodo
        done
        ;;
    'status')
        for i in $SLAVES;do
            pidfile="$PIDFILE_PREFIX/${i%%:*}.pid"
            errlog="$LOGFILE_PREFIX/${i%%:*}.errlog"
            stderr="$LOGFILE_PREFIX/${i%%:*}.stderr"
            nid="${i#*:}"
            start-stop-daemon --status --pidfile="$pidfile" --exec="$EXE" \
                --user=mysql --group=mysql --oknodo
        done
        ;;
    *)
        echo invalid arguments
        exit 1
        ;;
esac

exit 0
