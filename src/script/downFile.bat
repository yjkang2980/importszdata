@echo off
::����dmz�����������ftp��ת���ļ�������Ŀ¼�����Զ�����ѹ����ͬʱɾ������Ҫ��status�ļ�
cd C:\mdcdata
set year=%DATE:~0,4% 
set mad=%DATE:~5,2%%DATE:~8,2%
::set mad=0322
set var=0  
echo "�ļ�ת����ʼʱ��:%TIME%----/%year%/%mad%"
if not exist %year%\%mad% md %year%\%mad%
:continue  
echo ִ��%var%��
set /a var+=1  
ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn %DATE:~0,4% /%DATE:~0,4%/%DATE:~5,2%%DATE:~8,2%
if errorlevel 1 goto continue   

::ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn %year%/%mad% /%DATE:~0,4%/%DATE:~5,2%%DATE:~8,2%
::ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn 2018 /2018/%mad%
::if not exist %mad% md %mad%
::for /f "delims=" %i in ('ncftpls -u htzq -p bsQnPe91I09T ftp://sjfwfz.cninfo.com.cn/2018/%mad%') do ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn 2018/%mad% /2018/%i

echo "�ļ�ת�����ʱ��:%TIME%"

echo "��ʼ��ѹʱ��: %TIME%"
cd %year%/%mad%
7z x *.001 -odata -aoa

echo => ok

del /q C:\mdcdata\%DATE:~0,4%\%DATE:~5,2%%DATE:~8,2%\data\*status*

echo "������ѹʱ��: %TIME%"

::if not exist %DATE:~5,2%%DATE:~8,2% md %DATE:~5,2%%DATE:~8,2%
::if not exist %mad% md %mad%
::for /f "delims=" %i in ('ncftpls -u htzq -p bsQnPe91I09T ftp://sjfwfz.cninfo.com.cn/2018/0326') do ncftpget -d ftp.log -u htzq -p bsQnPe91I09T -R sjfwfz.cninfo.com.cn 2018/%mad% /2018/%i


