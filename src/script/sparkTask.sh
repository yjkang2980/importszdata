#/bin/bash
#用spark对数据文件进行校验和缺失导入，缺失导入类四个参数分别表示：年、月、日、是否在校验完成后将缺失数据入库,T表示入库
echo  "开始进行数据校验"
cd /home/u010571/dataretreive

year=`date +%Y`
month=`date +%m`
day=`date +%d`


echo "开始进行stock类型的行情校验......"
   spark-submit --class cn.com.htsc.hqcenter.dataretrive.SZSnapDataFileAndHBaseCom --master yarn-client --num-executors 100 --executor-memory 8G importszdata-1.0-SNAPSHOT.jar $year $month $day F
echo "完成股票类型的行情，开始逐笔成交类型的校验"
   spark-submit --class cn.com.htsc.hqcenter.dataretrive.SZTradeDataFileAndHBaseCom --master yarn-client --num-executors 100 --executor-memory 8G importszdata-1.0-SNAPSHOT.jar $year $month $day F
echo "完成逐笔成交类型的校验，开始逐笔委托类型的校验"
   spark-submit --class cn.com.htsc.hqcenter.dataretrive.SZOrderDataFileAndHBaseCom --master yarn-client --num-executors 100 --executor-memory 8G importszdata-1.0-SNAPSHOT.jar $year $month $day F
echo "完成逐笔委托类型的校验，开始指数类型的校验"
    spark-submit --class cn.com.htsc.hqcenter.dataretrive.SZIndexDataFileAndHBaseCom --master yarn-client --num-executors 100 --executor-memory 8G importszdata-1.0-SNAPSHOT.jar $year $month $day F
echo "完成指数类型的校验......"
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
