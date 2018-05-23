#/bin/bash
echo "四个参数，分别代表年月日 是否校验后插入数据，参数样例：2018 03 01 T"
echo  "开始进行数据校验"
cd /home/u010571/dataretreive

#DATE1=`date -d '1 days ago' +%Y%m%d`
#year=${DATE1:0:4}
#month=${DATE1:4:2}
#day=${DATE1:6:2}
#F=F



year=`date +%Y`
month=`date +%m`
day=`date +%d`
F=F

if [ $# -eq 4 ] ; then
year=$1
month=$2
day=$3
F=$4
fi 

echo "spark 任务-year:$year-month:$month-day:$day-F:$F"

echo "开始进行stock类型的行情校验......"
   spark-submit --jars /home/u010571/dataretreive/commons-net-3.5.jar --class cn.com.htsc.hqcenter.dataretrive.SZSnapDataFileAndHBaseCom --master yarn-client --num-executors 100 --executor-memory 8G importszdata-1.0-SNAPSHOT.jar $year $month $day $F
echo "完成股票类型的行情重新插入"
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                                                                                                                                                                                                                               
~                        
