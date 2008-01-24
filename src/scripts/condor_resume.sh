#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

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

$condor_bin/condor_release $target $id >&/dev/null

if [ "$?" == "0" ]; then
    echo " 0 No\\ error"
    exit 0
else
    echo " 1 Error"
    exit 1
fi

