@echo off
::内网dmz区机器从深交所ftp上转移文件到本地目录，且自动化解压缩，同时删除不需要的status文件
cd C:\mdcdata
set year=%DATE:~0,4% 
set mad=%DATE:~5,2%%DATE:~8,2%
::set mad=0322
set var=0  
echo "文件转移起始时间:%TIME%----/%year%/%mad%"
if not exist %year%\%mad% md %year%\%mad%
:continue  
echo 执行%var%次
set /a var+=1  
ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn %DATE:~0,4% /%DATE:~0,4%/%DATE:~5,2%%DATE:~8,2%
if errorlevel 1 goto continue   

::ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn %year%/%mad% /%DATE:~0,4%/%DATE:~5,2%%DATE:~8,2%
::ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn 2018 /2018/%mad%
::if not exist %mad% md %mad%
::for /f "delims=" %i in ('ncftpls -u htzq -p bsQnPe91I09T ftp://sjfwfz.cninfo.com.cn/2018/%mad%') do ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn 2018/%mad% /2018/%i

echo "文件转移完成时间:%TIME%"

echo "开始解压时间: %TIME%"
cd %year%/%mad%
7z x *.001 -odata -aoa

echo => ok

del /q C:\mdcdata\%DATE:~0,4%\%DATE:~5,2%%DATE:~8,2%\data\*status*

echo "结束解压时间: %TIME%"

::if not exist %DATE:~5,2%%DATE:~8,2% md %DATE:~5,2%%DATE:~8,2%
::if not exist %mad% md %mad%
::for /f "delims=" %i in ('ncftpls -u htzq -p bsQnPe91I09T ftp://sjfwfz.cninfo.com.cn/2018/0326') do ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn 2018/%mad% /2018/%i


