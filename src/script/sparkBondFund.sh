#/bin/bash
echo "债券基金倒入四个参数，分别代表年月日 是否校验后插入数据，参数样例：2018 03 01 T"
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
F=T

if [ $# -eq 4 ] ; then
year=$1
month=$2
day=$3
F=$4
fi 

echo "spark 任务-year:$year-month:$month-day:$day-F:$F"

echo "开始进行债券类型的行情校验......"
   spark-submit --jars /home/u010571/dataretreive/commons-net-3.5.jar --class cn.com.htsc.hqcenter.dataretrive.SZBondDataFileAndHBaseCom --master yarn-client --num-executors 100 --executor-memory 8G importszdata-1.0-SNAPSHOT.jar $year $month $day $F
echo "完成债券类型的行情，开始基金类型的校验"
   spark-submit --jars /home/u010571/dataretreive/commons-net-3.5.jar --class cn.com.htsc.hqcenter.dataretrive.SZFundDataFileAndHBaseCom --master yarn-client --num-executors 100 --executor-memory 8G importszdata-1.0-SNAPSHOT.jar $year $month $day $F
echo "完成基金类型的校验"
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
