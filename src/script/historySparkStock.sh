#!/bin/sh
date=`date -d "+0 day $1" +%Y%m%d`
enddate=`date -d "+1 day $2" +%Y%m%d`

echo "------------------------------"
echo "date=$date"
echo "enddate=$enddate"
echo "------------------------------"


while [[ $date < $enddate  ]]
do
        echo $date
         year=${date:0:4}
         month=${date:4:2}
         day=${date:6:2}
         #echo "开始：$year-$month--$day的数据倒入"
         hadoop fs -test -e /user/u010571/mddata/indexnew/$year/$month/$day
         if [ $? -eq 0 ]; then
        
          #    echo "$month $day 已经存在"
            echo "开始：$year-$month--$day的数据倒入"
           ./sparkStock.sh $year $month $day T >> historySparkStock.log 2>&1   
         fi
        #./sparkTask.sh $year $month $day T
        date=`date -d "+1 day $date" +%Y%m%d`

done
