#!/usr/bin/env sh

WEST_SERVERS="mysql mysql2 mysql3 mysql4 mysql5 mysql6 mysql7 mysql8 mysql9
mysql10 mysql11 mysql12 mysql13 mysql14 mysql17 mysql18 mysql19 mysql20 mysql23
mysql24 mysql25 mysql26 mysql27 mysql28 mysql29 mysql30 mysql31 mysql32"
EAST_SERVERS="mysql-east mysql2-east mysql3-east"
EU_SERVERS="mysql-eu mysql2-eu mysql3-eu mysql4-eu mysql5-eu mysql6-eu mysql7-eu
mysql8-eu mysql9-eu mysql10-eu mysql11-eu mysql12-eu mysql13-eu mysql14-eu 
mysql15-eu mysql16-eu mysql17-eu mysql18-eu"
SERVERS=

# Parse args
while getopts 'euw' o; do
    case "$o" in
        w) SERVERS="$SERVERS $WEST_SERVERS";;
        e) SERVERS="$SERVERS $EAST_SERVERS";;
        u) SERVERS="$SERVERS $EU_SERVERS";;
        [?]) echo "Usage: $0 [-w] [-e] [-u] command" >&2
		    exit 1;;
    esac
done
shift $((OPTIND-1))

if [[ "$@" == "" ]];then
    echo "command is empty!" >&2
    exit 1
fi

for i in $SERVERS;do
    echo [output from "$i"]:
    ssh "$i" "$@"
done
