#!/bin/sh
datearray=(0509 0510 0511 0514 0515)
for i in ${datearray[@]}
do
  mad=$i
  month=${i:0:2}
  day=${i:2:4}
  echo "mad:$mad--month:$month--day:$day"
  ./downloadData.sh 2018 $mad $month $day >> logdownhist.log 2>&1
#  ./sparkTask.sh 2018 $month $day F  >> logsparkhist.log 2>&1
done



