datestr=`date +%Y%m%d`
result=/analysis/output/personas/${datestr}/incdetail/datarelation/
localDir=/data/gss/result

function getresult(){
#从hdfs上拷贝明细结果
	$hadoop fs -test -d $result >> /dev/null 2>&1
	if [ $? = '0' ];then
	    deleteFile $localDir
	    mkdir -p $localDir
	   $hadoop fs -get $result/* $localDir/
	 else
	    echo "result not exit"
	  fi
}
getresult