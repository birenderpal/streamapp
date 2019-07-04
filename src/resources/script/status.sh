bin_dir=$(cd `dirname $0` && pwd)
base_dir=$(dirname $bin_dir)
app_name=$(basename $base_dir)".jar"
PIDS=`ps ax | grep -i $app_name | grep java | grep -v grep | awk '{print $1}'`
if [ -z "$PIDS" ]; then
  echo "$app_name NOT RUNNING"
else
      echo "$app_name RUNNING PID: $PIDS"
fi
