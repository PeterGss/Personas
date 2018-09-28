
datestr=20180926
longtime=`date +%s`

result=/analysis/output/personas/${datestr}/incdetail/datarelation/
localDir=/home/stars/octopod/bcpinput

function deleteFile()
{
	filename=$1
	#if [ -d "$filename" ]; then
	#fi
}
function getresult()
{
#从hdfs上拷贝明细结果
	hadoop fs -test -d $result >> /dev/null 2>&1
	if [ $? = '0' ];then
	    deleteFile $localDir
	    mkdir -p $localDir
	echo $localDir		 
	 hadoop fs -get $result/INDEXLEVEL_1_1_RELATION_* $localDir/
	 else
	    echo "result not exit"
	  fi

	  sequence=1
	  for tmpfile in $localDir/INDEXLEVEL_1_1_RELATION_*
        do
          file=INDEXLEVEL_1_1_RELATION_${longtime}_${sequence}.bcp
        echo $tmpfile $tmpfile/$file  
	mv ${tmpfile}   ${localDir}/${file}
          sequence=$(( sequence + 1 ))
        done
}
getresult
