#!/bin/bash

condor_config=`grep con_config ${GLITE_LOCATION:-/opt/glite}/etc/batch_gahp.config | grep -v \# | awk -F"=" '{print $2}' | sed -e 's/ //g' | sed -e 's/\"//g'`/
bin=`grep con_binpath ${GLITE_LOCATION:-/opt/glite}/etc/batch_gahp.config | grep -v \# | awk -F"=" '{print $2}' | sed -e 's/ //g' | sed -e 's/\"//g'`/

# The first and only argument is a JobId whose format is: Id/Queue/Pool

id=${1%%/*} # Id, everything before the first / in Id/Queue/Pool
queue_pool=${1#*/} # Queue/Pool, everything after the first /  in Id/Queue/Pool
queue=${queue_pool%/*} # Queue, everything before the first / in Queue/Pool
pool=${queue_pool#*/} # Pool, everything after the first / in Queue/Pool

if [ -z "$queue" ]; then
    target=""
else
    if [ -z "$pool" ]; then
	target="-name $queue"
    else
	target="-pool $pool -name $queue"
    fi
fi

$bin/condor_release $target $id >&/dev/null

if [ "$?" == "0" ]; then
    echo " 0 No\\ error"
    exit 0
else
    echo " 1 Error"
    exit 1
fi

