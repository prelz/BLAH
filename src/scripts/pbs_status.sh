#!/bin/bash

#  File:     pbs_status.sh
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
#
# Initialize env
if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

if [ ! -z "$PBS_BIN_PATH" ]; then
    binpath=${PBS_BIN_PATH}/
else
    binpath=/usr/pbs/bin/
fi

if [ ! -z "$PBS_SPOOL_DIR" ]; then
    spoolpath=${PBS_SPOOL_DIR}/
else
    spoolpath=/usr/spool/PBS/
fi

pars=$*
requested=`echo $pars | sed 's/^.*\///'`
logfile=`echo $pars | sed 's/\/.*//'`

# Try with qstat first, if job is found
# format output as a classad
result=`${binpath}qstat -f $requested | awk '
BEGIN {
    current_job = ""
    current_attr = ""
    current_value = ""
    curent_delimiter = ""
    value_ended = 1
}

function new_attr(attr_name, delimiter) {
    if (current_attr != "") {
        print current_attr " = " current_delimiter current_value current_delimiter ";"
        value_ended = 1
    }
    current_attr = attr_name
    current_value = substr($0, index($0, "=") + 2)
    current_delimiter = delimiter
}

/Job Id:/ {
    current_job = substr($0, index($0, ":") + 2)
    print "[\nBatchjobId = \"" current_job "\";"
}

/Error_Path =/     { new_attr("Err", "\"") }
/Output_Path =/    { new_attr("Out", "\"") }
/etime =/          { new_attr("Started", "\"") }
/job_state =/      { new_attr("JobStatus", "")
                     if (current_value == "Q") current_value = 1
                     if (current_value == "R") current_value = 2
                     if (current_value == "H") current_value = 5
                   }

/^$/ {
    print current_attr " = " current_delimiter current_value current_delimiter ";"
    value_ended = 1
    print "]"
}
'
`
echo $result

# If qstat doesn't know about the job,
# let's search it in log files
if [ -z "$result" ]; then
  # Try to find an exit code in log files
  logpath=${spoolpath}server_logs
  logs=$logpath/$logfile
  logs="$logs "`find $logpath -type f -newer $logs`
  exitcode=`grep "$requested;Exit_status=" $logs | sed 's/.*Exit_status=\([0-9]*\).*/\1/'`
  if [ -n "$exitcode" ]
  then
    cat <<FINE_OK
[
BatchjobId = "$requested";
ExitCode = $exitcode;
JobStatus = 4;
]
FINE_OK
    retcode=0
  # Try to see if job has been deleted
  elif grep -q "$requested;Job deleted" $logs
  then
    cat <<FINE_DEL
[
BatchjobId = "$requested";
JobStatus = 3;
]
FINE_DEL
    retcode=0
  else
    echo "Error: cannot retrieve job status"
    retcode=1
  fi
fi

echo \*\*\* END \*\*\*
exit $retcode
