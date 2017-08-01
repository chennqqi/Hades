#!/bin/sh

#Save two days, more than the number of days will be automatically deleted
filesavedays=2

#The working directory for the Acquisition server
workpath=/home/nebula/outdata/szanalysis_v2.0/
AOTAIN_SZUSERANALYSIS_LOG=/home/nebula/log/szanalysis_v2.0/szuseranalysisftp.log
#Cycle time to run this script. (unit: second)
cirtime=60


#ftp
ftptransfer()
{
	filename=$1
	localdir=$2
	ftp -i -n 219.134.184.76 << EOFa
	user aotian shenzhen755
	cd /report
	lcd ${localdir}
        bin
	put ${filename}
	quit
EOFa
}

mvfile()
{
	file=$1
	tar -zcvf ${file}.gz ${file}
	ftptransfer ${file}.gz ${workpath}

        echo -e `date +"%Y%m%d%H%M%S"` "Compression and transfer file:"${file} >>${AOTAIN_SZUSERANALYSIS_LOG}
	rm -f ${file}
        mv ${file}.gz /home/nebula/outdata/bak/
	#rm -f ${file}.gz
}

#Upload has compressed files
echo -e `date +"%Y%m%d%H%M%S"` "Upload file to Shenzhen Telecom servers start">>${AOTAIN_SZUSERANALYSIS_LOG}

#while true
#do
	cd ${workpath}
	filenames=`ls -l *.txt | awk '{print $9}'`
	#Using the fuser function to determine whether the file is being used, if not, execute mvfile

	for file in ${filenames}
	do
		fuser ${file} || mvfile ${file}
	done

	#Delete Expired Files
	#find ${workpath} -mtime +${filesavedays} -name "*" -exec rm -rf {} \;
        echo -e `date +"%Y%m%d%H%M%S"` "Upload end" >>${AOTAIN_SZUSERANALYSIS_LOG}       
#	sleep ${cirtime}
#done

exit 0


