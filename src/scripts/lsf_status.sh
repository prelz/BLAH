#!/bin/bash

#  File:     lsf_status.sh
#
#  Author:   David Rebatto, Massimo Mezzadri
#  e-mail:   David.Rebatto@mi.infn.it, Massimo.Mezzadri@mi.infn.it
#
#
#  Revision history:
#    20-Mar-2004: Original release
#    22-Feb-2005: Totally rewritten, bhist command not used anymore
#     3-May-2005: Added support for Blah Log Parser daemon (using the BLParser flag)
#
#  Description:
#    Return a classad describing the status of a LSF job
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

if [ ! -z "$LSF_BIN_PATH" ]; then
	binpath=${LSF_BIN_PATH}/
else
	binpath=/usr/local/lsf/bin/
fi

usage_string="Usage: $0 [-w]"

#get worker node info (dummy for LSF)
getwn=""

#set to yes if BLParser is present in the installation 
BLParser=""

BLPserver="127.0.0.1"
BLPport=33333
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
requested=`echo $pars | sed -e 's/^.*\///'`
datenow=`echo $pars | sed 's/\/.*//'`

datefile=`mktemp -q blahjob_XXXXXX`
if [ $? -ne 0 ]; then
   echo 'Error creating temporary file'
   exit 1
fi

proxy_dir=~/.blah_jobproxy_dir

$?=0

if [ "x$BLParser" == "xyes" ] ; then

    usingBLP="yes"
    result=`echo $pars| $BLClient -a $BLPserver -p $BLPport`
fi

if [ "$?" =="1" || "x$BLParser" != "xyes"] ; then

usingBLP="no"
confpath=${LSF_CONF_PATH:-/etc}
conffile=$confpath/lsf.conf

lsf_base_path=`cat $conffile|grep LSB_SHAREDIR| awk -F"=" '{ print $2 }'`

lsf_clustername=`${binpath}lsid | grep 'My cluster name is'|awk -F" " '{ print $5 }'`
logpath=$lsf_base_path/$lsf_clustername/logdir

logeventfile=lsb.events

touch -t $datenow $datefile
ulogs=`find $logpath/$logeventfile.[0-9]* -type f -newer $datefile -print`
rm $datefile

for i in `echo $ulogs | sed "s|${logpath}/${logeventfile}\.||g" | sort -nr`; do
 logs="$logs$logpath/$logeventfile.$i "
done

logs="$logs$logpath/$logeventfile"

#/* job states */
#define JOB_STAT_NULL         0x00
#define JOB_STAT_PEND         0x01
#define JOB_STAT_PSUSP        0x02
#define JOB_STAT_RUN          0x04
#define JOB_STAT_SSUSP        0x08
#define JOB_STAT_USUSP        0x10
#define JOB_STAT_EXIT         0x20
#define JOB_STAT_DONE         0x40
#define JOB_STAT_PDONE        (0x80)  /* Post job process done successfully */
#define JOB_STAT_PERR         (0x100) /* Post job process has error */
#define JOB_STAT_WAIT         (0x200) /* Chunk job waiting its turn to exec */
#define JOB_STAT_UNKWN        0x10000

result=`awk -v jobId=$requested -v proxyDir=$proxy_dir '
BEGIN {
	rex_queued   = "\"JOB_NEW\" \"[0-9\.]+\" [0-9]+ " jobId
	rex_running  = "\"JOB_START\" \"[0-9\.]+\" [0-9]+ " jobId
	rex_deleted  = "\"JOB_SIGNAL\" \"[0-9\.]+\" [0-9]+ " jobId "[0-9]+ [0-9]+ \"KILL\""
	rex_done     = "\"JOB_STATUS\" \"[0-9\.]+\" [0-9]+ " jobId " 192 "
	rex_finished = "\"JOB_STATUS\" \"[0-9\.]+\" [0-9]+ " jobId " 32 "
	rex_hold     = "\"JOB_STATUS\" \"[0-9\.]+\" [0-9]+ " jobId " 16 "
	rex_resume   = "\"JOB_STATUS\" \"[0-9\.]+\" [0-9]+ " jobId " 4 "
        
	jobstatus = 0
	
	print "["
	print "BatchjobId = \"" jobId "\";"
}

$0 ~ rex_queued {
	jobstatus = 1
}

$0 ~ rex_running {
	jobstatus = 2
        print "WorkerNode = " $10 ";"
}

$0 ~ rex_deleted {
	jobstatus = 3
	exit
}

$0 ~ rex_done {
	jobstatus = 4
	exitcode = 0
	exit
}

$0 ~ rex_finished {
	jobstatus = 4
	exitcode = $(NF-1)
	exit
}

$0 ~ rex_hold {
	jobstatus = 5
}

$0 ~ rex_resume {
	jobstatus = 2
}

END {
	if (jobstatus == 0) { exit 1 }
	print "JobStatus = " jobstatus ";"
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
        echo $result
        retcode=0
   else
        echo "ERROR: Job not found"
        retcode=1
   fi
  
   exit $retcode

fi #close if on BLParser

if [ "x$usingBLP" == "xyes" ] ; then

    pr_removal=`echo $result | sed -e 's/^.*\///'`
    result=`echo $result | sed 's/\/.*//'`

    if [ "x$pr_removal" == "xYes" ] ; then
        rm ${proxy_dir}/${requested}.proxy 2>/dev/null

    fi

        echo $result
        exit $retcode

fi








