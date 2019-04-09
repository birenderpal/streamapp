#!/bin/bash
bin_dir=$(dirname $0)
LOGFILE=$(dirname $bin_dir)/logs/monitor.log

app_name="SplunkStreamApp.jar"
TSTAMP=`date`
PIDS=`ps ax | grep -i $app_name | grep java | grep -v grep | awk '{print $1}'`
if [ -z "$PIDS" ]; then
  echo "$TSTAMP : No $app_name stream application running" >> $LOGFILE
  echo "$TSTAMP : starting the stream app" >> $LOGFILE
  /opt/SplunkApp/bin/init.sh 6
else
  echo "$TSTAMP : $app_name Running" >> $LOGFILE
fi

