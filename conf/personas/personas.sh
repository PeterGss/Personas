#!/bin/bash
source /etc/profile.d/fhbase_env.sh
importerAddr=$( echo $FHBASE_IMPORTER | awk -F, '{print $1}' )
resourcename=samefamily
resoucenameuppercase=`echo ${resourcename}|tr a-z A-Z`
datestr=`date +%Y%m%d`
longtime=`date +%s`
result=/analysis/output/together/${resourcename}/${datestr}
hadoop=/cluster/hadoop/bin/hadoop
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

function inputStatDate()
{
  pathname=$1
  # 从hdfs上拷贝统计结果数据
  $hadoop fs -test -d $result/$pathname >> /dev/null 2>&1
  if [ $? = '0' ];then
    deleteFile $stalocaldir/$pathname
    mkdir -p $stalocaldir/$pathname
    $hadoop fs -get $result/$pathname/* $stalocaldir/$pathname
  else 
    echo "result not exit"
    exit -1;
  fi

  ##创建统计文件目录，并获取统计文件文件名
  statfilename=MASS_${longtime}_0_BCPSTAT_${longtime}_0.bcp
  statdir=$localDir/statdir
  mkdir -p $statdir

  sequence=1
  for tmpfile in $stalocaldir/$pathname/*
  do
    file=GRAPHDB_SOCIETY_$2_${longtime}_${sequence}.bcp
    ## 写统计文件
    num=`cat $tmpfile | wc -l`
    printf "${file}\t${num}\tOUT\tFHBASE\t${longtime}\n">>${statdir}/${statfilename}.temp
    ## 发结果数据文件给入库机
    echo $localDir/${file}.temp
    echo $tmpfile
    mv $tmpfile $localDir/${file}.temp
    scp $localDir/${file}.temp $importerAddr:$graphdbdir
    ssh $importerAddr "mv ${graphdbdir}/${file}.temp ${graphdbdir}/${file}"
    sequence=$(( sequence + 1 ))
  done

  ## 发统计文件给入库机
  if [ -f ${statdir}/${statfilename}.temp ]
  then
  scp ${statdir}/${statfilename}.temp $importerAddr:$graphdbdir
  ssh $importerAddr "mv ${graphdbdir}/${statfilename}.temp ${graphdbdir}/${statfilename}"
  fi
}

inputDetailDate
inputStatDate marriage SAMEMARRY
inputStatDate divorce SAMEDIVORCE