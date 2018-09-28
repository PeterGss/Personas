
datestr=20180927
longtime=`date +%s`

result=/analysis/output/personas/${datestr}/incdetail/datarelation/
localDir=/data/gss/result

function deleteFile()
{
	filename=$1
	if [ -d "$filename" ]; then
		rm -rf $filename
	fi
}
function getresult()
{
#从hdfs上拷贝明细结果
	hadoop fs -test -d $result >> /dev/null 2>&1
	if [ $? = '0' ];then
	    deleteFile $localDir
	    mkdir -p $localDir
	   hadoop fs -get $result/INDEXLEVEL* $localDir/
	 else
	    echo "result not exit"
	  fi

	  sequence=1
	  for tmpfile in localDir/INDEXLEVEL*
        do
          file=INDEXLEVEL_1_1_RELATION_${longtime}_${sequence}.bcp
          mv $tmpfile $file
          sequence=$(( sequence + 1 ))
        done
}
getresult