#!/bin/bash

#  File:     lsf_status.sh
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#
#  Description:
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#

if [ ! -z "$LSF_BIN_PATH" ]; then
    binpath=${LSF_BIN_PATH}/
else
    binpath=/usr/local/lsf/bin/
fi

pars=$*
requested=`echo $pars | sed 's/^.*\///'`
logfile=`echo $pars | sed 's/\/.*//'`

# bhist output formatted as a classad

typeset -i lognum=`echo $logfile | sed 's/^.*\.//'`+1

result=`${binpath}bhist -n $lognum -l $requested | awk '
/^[^ ]/ {
    print current_value 
    current_value = $0
}
/^[ ]+/ {
    current_value = current_value substr($0, 22)
}
END{
 print current_value 
}
'|awk '
BEGIN {
    current_job = ""
    current_out = ""
    current_err = ""
    current_time = ""
    current_exit = ""
    current_status = "1"
}

/^Job/ {
    current_job = substr($0, index($0, "Job <") + 5, index($0, ">") - index($0, "Job <") - 5)
    print "[\nBatchjobId = \"" current_job "\";"
}

/Output File/  {
    current_out = substr($0, index($0, "Output File <") + 13, index($0, ">, Error File") - index($0, "Output File <") - 13)
    print "Out = \"" current_out "\";"
}

/Error File/ {
    current_err = substr($0, index($0, "Error File <") + 12, index($0, ">, Notify") - index($0, "Error File <") - 12)
    if(current_err) {
     print "Err = \"" current_err "\";"
    }else{
     current_err = substr($0, index($0, "Error File <") + 12, index($0, ">, Copy") - index($0, "Error File <") - 12)
     print "Err = \"" current_err "\";"
    }
}

/Start/ {
    current_time = substr($0, 0, index($0, "Start") - 3)
    print "Started = \"" current_time "\";"
    current_status = "2"
}

/Suspended by the user/ {
  current_status = "5"
}

/Done successfully/ {
    current_exit = 0
    print "ExitCode = " current_exit ";"
    current_status = "4"
}

/Exited with exit code/ {
    current_exit = substr($0, index($0, "Exited with exit code") + 22, index($0, ".") - index($0, "Exited with exit code") - 22)
    print "ExitCode = " current_exit ";"
    if(current_status == 3){
     current_status = "3"
    }else{
     current_status = "4"
    }
}

/Signal <KILL>/ {
  current_status = "3"
}

/No matching job found/ {
  current_status = "-1"
}

END {
    if (current_status != -1){ 
    print "JobStatus = " current_status ";"
    print "]"
    }
}
'
`
# If bhist doesn't know about the job,
if [ "x$result" == "x" ]; then
 echo "Error: cannot retrieve job status"
 retcode=1
else 
 retcode=0
 echo $result
fi

echo \*\*\* END \*\*\*
exit $retcode
