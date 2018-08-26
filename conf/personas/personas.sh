#!/bin/bash
source /etc/profile.d/fhbase_env.sh
importerAddr=
resourcename=personas
resoucenameuppercase=`echo ${resourcename}|tr a-z A-Z`
datestr=`date +%Y%m%d`
longtime=`date +%s`
result=/analysis/output/together/${resourcename}/${datestr}
hadoop=hadoop
##本地路径，用来临时保存结果数据
businessName=mongo_result
inputdir=/bcptemp/jettinput
graphdbdir=/bcptemp/${businessName}
localDir=/hdata/${businessName}/together/${resourcename}
stalocaldir=$localDir/statisticresult
inclocaldir=$localDir/incresult
if [ -d $localDir ];then
    rm -rf $localDir
	echo delete $localDir
fi

function deleteFile()
{
	filename=$1
	if [ -d "$filename" ]; then 
		rm -rf $filename
	fi
}

function inputDetailDate()
{
  # 从hdfs上拷贝明细结果数据
  $hadoop fs -test -d $result >> /dev/null 2>&1
  if [ $? = '0' ];then
          deleteFile $inclocaldir
          mkdir -p $inclocaldir
          $hadoop fs -get $result/incdetail/* $inclocaldir/
  else
          echo "result not exit"
  fi

  #拷贝明细数据
  sequence=1
  for tmpfile in $inclocaldir/*
  do
    file=MASS_1_${resoucenameuppercase}DETAIL_${longtime}_${sequence}.BCP
    ## 发结果数据文件给入库机
    echo $tmpfile $file
    mv $tmpfile $localDir/${file}.temp
    scp $localDir/${file}.temp $importerAddr:${inputdir}/
    ssh $importerAddr "mv ${inputdir}/${file}.temp ${inputdir}/${file}"
    sequence=$(( sequence + 1 ))
  done
}


inputDetailDate
