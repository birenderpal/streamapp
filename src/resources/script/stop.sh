SIGNAL=${SIGNAL:-TERM}
app_name="SplunkStreamApp.jar"
PIDS=ps ax | grep -i $app_name | grep java | grep -v grep | awk '{print $1}'
if [ -z "$PIDS" ]; then
  echo "No $app_name stream application to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
