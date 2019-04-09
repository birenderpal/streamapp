bin_dir=$(dirname $0)
base_dir=$(dirname $bin_dir)
#app_name=$(basename $base_dir)".jar"a
app_name="SplunkStreamApp.jar"
if [ $# -lt 1 ];
then
        echo "USAGE: $0 [number of instance]"
        exit 1
fi
instance=$1
while [ $instance -gt 0 ];
do
  java -jar -Djava.io.tmpdir=$base_dir -Dapp.log.dir=$bin_dir/../logs -Dlog4j.configurationFile=$base_dir/conf/log4j.properties  $bin_dir/../$app_name &
  instance=$(($instance-1))
done


