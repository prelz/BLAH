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
#    03-May-2005: Added support for Blah Log Parser daemon (using the pbs_BLParser flag)
#
#  Description:
#    Return a classad describing the status of a PBS job
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

usage_string="Usage: $0 [-w] [-n]"

logpath=${pbs_spoolpath}/server_logs

#get worker node info
getwn=""

#get creamport
getcreamport=""

usedBLParser="no"

srvfound=""

BLClient="${GLITE_LOCATION:-/opt/glite}/bin/BLClient"

###############################################################
# Parse parameters
###############################################################

while getopts "wn" arg 
do
    case "$arg" in
    w) getwn="yes" ;;
    n) getcreamport="yes" ;;
    
    -) break ;;
    ?) echo $usage_string
       exit 1 ;;
    esac
done

shift `expr $OPTIND - 1`

#Try different logparser
if [ ! -z $pbs_num_BLParser ] ; then
 for i in `seq 1 $pbs_num_BLParser` ; do
  s=`echo pbs_BLPserver${i}`
  p=`echo pbs_BLPport${i}`
  eval tsrv=\$$s
  eval tport=\$$p
  testres=`echo "TEST/"|$BLClient -a $tsrv -p $tport`
  if [ "x$testres" == "xYPBS" ] ; then
   pbs_BLPserver=$tsrv
   pbs_BLPport=$tport
   srvfound=1
   break
  fi
 done
 if [ -z $srvfound ] ; then
  echo "1ERROR: not able to talk with no logparser listed"
  exit 0
 fi
fi

###################################################################
#get creamport and exit

if [ "x$getcreamport" == "xyes" ] ; then
 result=`echo "CREAMPORT/"|$BLClient -a $pbs_BLPserver -p $pbs_BLPport`
 reqretcode=$?
 if [ "$reqretcode" == "1" ] ; then
  exit 1
 fi
 retcode=0
 echo $pbs_BLPserver:$result
 exit $retcode
fi

pars=$*
proxy_dir=~/.blah_jobproxy_dir

for  reqfull in $pars ; do
	requested=""
	#header elimination
	requested=${reqfull:4}
	reqjob=`echo $requested | sed -e 's/^.*\///'`
	logfile=`echo $requested | sed 's/\/.*//'`
	if [ "x$getwn" == "xyes" ] ; then
		workernode=`${pbs_binpath}/qstat -f $reqjob 2> /dev/null | grep exec_host| sed "s/exec_host = //" | awk -F"/" '{ print $1 }'`
	fi

	cliretcode=0
	retcode=0
	logs=""
	result=""
	logfile=`echo $requested | sed 's/\/.*//'`
	if [ "x$pbs_BLParser" == "xyes" ] ; then
    		usedBLParser="yes"
		result=`echo $requested | $BLClient -a $pbs_BLPserver -p $pbs_BLPport`
		cliretcode=$?
		response=${result:0:1}
		if [ "$response" != "[" -o "$cliretcode" != "0" ] ; then
			cliretcode=1
		else 
			cliretcode=0
		fi
	fi
	if [ "$cliretcode" == "1" -a "x$pbs_fallback" == "xno" ] ; then
	 echo "1ERROR: not able to talk with logparser on ${pbs_BLPserver}:${pbs_BLPport}"
	 exit 0
	fi
	if [ "$cliretcode" == "1" -o "x$pbs_BLParser" != "xyes" ] ; then
		result=""
		usedBLParser="no"
		logs="$logpath/$logfile `find $logpath -type f -newer $logpath/$logfile`"
		result=`awk -v jobId="$reqjob" -v wn="$workernode" -v proxyDir="$proxy_dir" '
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
	fi #close if on pbs_BLParser
	if [ "x$usedBLParser" == "xyes" ] ; then
		pr_removal=`echo $result | sed -e 's/^.*\///'`
    		result=`echo $result | sed 's/\/.*//'`
		echo "0"$result "Workernode=\"$workernode\";]"
		if [ "x$pr_removal" == "xYes" ] ; then
        		rm ${proxy_dir}/${reqjob}.proxy 2>/dev/null
    		fi
		usedBLParser="no"	
	fi
done 
exit 0

