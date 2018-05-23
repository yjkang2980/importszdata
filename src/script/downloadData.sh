#!/bin/bash
#将数据文件从dmz区机器转移到内网cdh客户机上，上传至hdfs制定目录位置，并且将文件映射为hive外表
upToHadoop(){
  hadoop fs -test -e /user/u010571/mddata/$1new/$year/$month/$day
  if [ $? -eq 0 ]; then
     echo "$month $day 已经存在"
  else
     echo "$year $day不存在，需要创建"
     
     hadoop fs -mkdir /user/u010571/mddata/$1new/$year
     hadoop fs -mkdir /user/u010571/mddata/$1new/$year/$month
     hadoop fs -mkdir /user/u010571/mddata/$1new/$year/$month/$day
  fi
  hadoop fs -put -f $2 /user/u010571/mddata/$1new/$year/$month/$day/$2
  
  if [ $? -eq 0 ];then
  echo "上传到hadoop成功，删除本地文件$2"    
    rm -f $2
  fi
  echo "完成$year $day 的$2上传"

}


echo "开始进行ftp数据传输到本地"
year=`date +%Y`
mad=`date +%m%d`
month=`date +%m`
day=`date +%d`
#year=2018
#mad=0330
#month=03
#day=30

cd /home/u010571/dataretreive
while [ ! -f "./data/ok"  ]; do 
      echo "深交所文件还没有完成到内网的转移，等待完成"
      sleep 20s
      wget -nH -N --ftp-user=appadmin --ftp-password=appadmin ftp://168.8.3.50/$year/$mad/ok -Pdata
done

echo "深交所文件完成到内网的转移，开始往cdh客户机上转移......"

wget -nH -N --ftp-user=appadmin --ftp-password=appadmin ftp://168.8.3.50/$year/$mad/data/* -Pdata

echo "完成本地文件的传输......"

cd data

for i in `ls -f`
do
    if [[ $i =~ "index" ]]
    then
      upToHadoop index $i
    fi
   if [[ $i =~ "order" ]]
    then
      upToHadoop order $i
    fi
    if [[ $i =~ "trade" ]]
    then
      upToHadoop trade $i
    fi

   if [[ $i =~ "snap_spot" ]]
    then
     upToHadoop snapshot $i
    fi

   if [[ $i =~ "snap_level" ]]
    then
      upToHadoop snapshotlevel $i
    fi
done
echo " 完成上传到大数据平台上......"
rm -f *

hive -e "use mdc;alter table table_app_t_md_index add partition (year='$year', month='$month', day='$day') location '$year/$month/$day'"
hive -e "use mdc;alter table table_app_t_md_order add partition (year='$year', month='$month', day='$day') location '$year/$month/$day'"
hive -e "use mdc;alter table table_app_t_md_trade add partition (year='$year', month='$month', day='$day') location '$year/$month/$day'"
hive -e "use mdc;alter table table_app_t_md_snap add partition (year='$year', month='$month', day='$day') location '$year/$month/$day'"
hive -e "use mdc;alter table table_app_t_md_snap_level add partition (year='$year', month='$month', day='$day') location '$year/$month/$day'"
echo "完成hive表的分区添加......;"



