bin_dir=$(cd `dirname $0` && pwd)
base_dir=$(dirname $bin_dir)
app_name=$(basename $base_dir)".jar"

java -jar -Djava.io.tmpdir=$base_dir -Dapp.log.dir=$bin_dir/../logs -Dlog4j.configurationFile=$base_dir/conf/log4j.properties  $bin_dir/../$app_name &


