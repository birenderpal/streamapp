#!/bin/bash
bin_dir=$(cd `dirname $0` && pwd)
base_dir=$(dirname $bin_dir)
app_name=$(basename $base_dir)".jar"
LOGFILE=$(dirname $bin_dir)/logs/monitor.log

TSTAMP=`date`
PIDS=`ps ax | grep -i $app_name | grep java | grep -v grep | awk '{print $1}'`
if [ -z "$PIDS" ]; then
  echo "$TSTAMP : No $app_name stream application running" >> $LOGFILE
  echo "$TSTAMP : starting the stream app" >> $LOGFILE
  $bin_dir/bin/init.sh
else
  echo "$TSTAMP : $app_name Running" >> $LOGFILE
fi

