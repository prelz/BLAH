#!/bin/bash

#  File:     pbs_status.sh
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#    20-Mar-2004: Original release
#    04-Jan-2005: Totally rewritten, qstat command not used anymore
#    03-May-2005: Added support for Blah Log Parser daemon (using the BLParser flag)
#
#  Description:
#    Return a classad describing the status of a PBS job
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#

if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

if [ ! -z "$PBS_SPOOL_DIR" ]; then
    spoolpath=${PBS_SPOOL_DIR}/
else
    spoolpath=/usr/spool/PBS/
fi

if [ ! -z "$PBS_BIN_PATH" ]; then
    pbsbinpath=${PBS_BIN_PATH}/
else
    pbsbinpath=/usr/pbs/bin/
fi

usage_string="Usage: $0 [-w]"

logpath=${spoolpath}server_logs

#get worker node info
getwn=""

#set to yes if BLParser is present in the installation
BLParser=""

BLPserver="127.0.0.1"
BLPport=33332
BLClient="${GLITE_LOCATION:-/opt/glite}/bin/BLClient"

###############################################################
# Parse parameters
###############################################################

while getopts "w" arg 
do
    case "$arg" in
    w) getwn="yes" ;;

    -) break ;;
    ?) echo $usage_string
       exit 1 ;;
    esac
done

shift `expr $OPTIND - 1`

###################################################################

pars=$*
#test per vedere se si possono leggere piu'jobids
for  reqfull in $pars ; do
        requested=`echo $reqfull | sed -e 's/^.*\///'`
	if [ "x$getwn" == "xyes" ] ; then
		 workernode=`${pbsbinpath}/qstat -f $requested 2> /dev/null | grep exec_host| sed "s/exec_host = //" | awk -F"/" '{ print $1 }'`
	fi

	proxy_dir=~/.blah_jobproxy_dir

	cliretcode=0
	if [ "x$BLParser" == "xyes" ] ; then

    	usingBLP="yes"
    	result=`echo $pars| $BLClient -a $BLPserver -p $BLPport`
    	cliretcode=$?

	fi
	if [ "$cliretcode" == "1" -o "x$BLParser" != "xyes" ] ; then

		usingBLP="no"
		logfile=`echo $pars | sed 's/\/.*//'`
		logs="$logpath/$logfile `find $logpath -type f -newer $logpath/$logfile`"

		result=`awk -v jobId="$requested" -v wn="$workernode" -v proxyDir="$proxy_dir" '
BEGIN {
	rex_queued   = jobId ";Job Queued "
	rex_running  = jobId ";Job Run "
	rex_deleted  = jobId ";Job deleted "
	rex_finished = jobId ";Exit_status="
	rex_hold     = jobId ";Holds "

	print "["
	print "BatchjobId = \"" jobId "\";"
}

$0 ~ rex_queued {
	jobstatus = 1
}

$0 ~ rex_running {
	jobstatus = 2
}

$0 ~ rex_deleted {
	jobstatus = 3
	exit
}

$0 ~ rex_finished {
	jobstatus = 4
	s = substr($0, index($0, "Exit_status="))
	s = substr(s, 1, index(s, " ")-1)
	exitcode = substr(s, index(s, "=")+1)
	exit
}

$0 ~ rex_hold {
	jobstatus = 5
}

END {
	if (jobstatus == 0) { exit 1 }
	print "JobStatus = " jobstatus ";"
	if (jobstatus == 2) {
		print "WorkerNode = \"" wn "\";"
	}
	if (jobstatus == 4) {
		print "ExitCode = " exitcode ";"
	}
	print "]"
	if (jobstatus == 3 || jobstatus == 4) {
		system("rm " proxyDir "/" jobId ".proxy 2>/dev/null")
	}
}
' $logs`

  		if [ "$?" == "0" ] ; then
			echo "0"$result
			retcode=0
  		else
			echo "1ERROR: Job not found"
			retcode=1
  		fi
  
  	#exit $retcode

	fi #close if on BLParser
	if [ "x$usingBLP" == "xyes" ] ; then

    		pr_removal=`echo $result | sed -e 's/^.*\///'`
    		result=`echo $result | sed 's/\/.*//'`

    		if [ "x$pr_removal" == "xYes" ] ; then
        		rm ${proxy_dir}/${requested}.proxy 2>/dev/null

    		fi
        	echo $result "Workernode=\"$workernode\";]"
        	#exit $retcode

	fi

done 

exit $retcode
