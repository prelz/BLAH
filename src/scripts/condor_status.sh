#!/bin/bash

#  File:     condor_status.sh
#
#  Author:   Matt Farrellee (Condor) - received on March 28, 2007
#
#
# Copyright (c) Members of the EGEE Collaboration. 2004. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
# 
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
# 
#     http://www.apache.org/licenses/LICENSE-2.0 
# 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.
#

proxy_dir=~/.blah_jobproxy_dir

. `dirname $0`/blah_load_config.sh

if [ "x$job_registry" != "x" ] ; then
   ${blah_sbin_directory}/blah_job_registry_lkup $@
   exit 0
fi

FORMAT='-format "%d" ClusterId -format "," ALWAYS -format "%d" JobStatus -format "," ALWAYS -format "%f" RemoteSysCpu -format "," ALWAYS -format "%f" RemoteUserCpu -format "," ALWAYS -format "%f" BytesSent -format "," ALWAYS -format "%f" BytesRecvd -format "," ALWAYS -format "%f" RemoteWallClockTime -format "," ALWAYS -format "%d" ExitBySignal -format "," ALWAYS -format "%d" ExitCode -format "%d" ExitSignal -format "\n" ALWAYS'

# The "main" for this script is way at the bottom of the file.

# Given job information generate an ad.
function make_ad {
    local job=$1
    local line=$2

    local cluster=$(echo $line | awk -F ',' '{print $1}')
    local status=$(echo $line | awk -F ',' '{print $2}')
    local remote_sys_cpu=$(echo $line | awk -F ',' '{print $3}')
    local remote_user_cpu=$(echo $line | awk -F ',' '{print $4}')
    local bytes_sent=$(echo $line | awk -F ',' '{print $5}')
    local bytes_recvd=$(echo $line | awk -F ',' '{print $6}')
    local remote_wall_clock_time=$(echo $line | awk -F ',' '{print $7}')
    local exit_by_signal=$(echo $line | awk -F ',' '{print $8}')
    local code_or_signal=$(echo $line | awk -F ',' '{print $9}')

    # Clean up proxy renewal links if applicable
    if [ "$status" == "3" -o "$status" == "4" ]; then
        /bin/rm -f $proxy_dir/$job.proxy.norenew 2>/dev/null
    fi

    echo -n "[BatchjobId=\"$job\";JobStatus=$status;RemoteSysCpu=${remote_sys_cpu:-0};RemoteUserCpu=${remote_user_cpu:-0};BytesSent=${bytes_sent:-0};BytesRecvd=${bytes_recvd:-0};RemoteWallClockTime=${remote_wall_clock_time:-0};"
    if [ "$status" == "4" ] ; then
	if [ "$exit_by_signal" == "0" ] ; then
	    echo -n "ExitBySignal=FALSE;ExitCode=${code_or_signal:-0}"
	else
	    echo -n "ExitBySignal=TRUE;ExitSignal=${code_of_signal:-0}"
	fi
    fi
    echo "]"
}

### main

while getopts "wn" arg 
do
    case "$arg" in
    w) ;;
    n) ;;
    -) break ;;
    ?) echo "Usage: $0 [-w] [-n]"
       exit 1 ;;
    esac
done

shift `expr $OPTIND - 1`

for job in $* ; do
# The job's format is: condor/Id/Queue/Pool
    job=${job#con*/} # Strip off the leading "con(dor)/"
    id=${job%%/*} # Id, everything before the first / in Id/Queue/Pool
    queue_pool=${job#*/} # Queue/Pool, everything after the first /  in Id/Queue/Pool
    queue=${queue_pool%/*} # Queue, everything before the first / in Queue/Pool
    pool=${queue_pool#*/} # Pool, everything after the first / in Queue/Pool

    if [ -z "$queue" -o "$condor_use_queue_as_schedd" != "yes" ]; then
	target=""
    else
	if [ -z "$pool" ]; then
	    target="-name $queue"
	else
	    target="-pool $pool -name $queue"
	fi
    fi

    # do an explicit condor_q for
    # this job before trying condor_history, which can take a long time.
    line=$(echo $FORMAT | xargs $condor_binpath/condor_q $target $id)
    if  [ -n "$line" ] ; then
       echo "0$(make_ad $job "$line")"
       exit 0
    fi

    ### WARNING: This is troubling because the remote history file
    ### might just happen to be in the same place as a local history
    ### file, in which case condor_history is going to be looking at
    ### the history of an unexpected queue.

    # We can possibly get the location of the history file and check it.
    # NOTE: In Condor 7.7.6-7.8.1, the -f option to condor_history was
    #   broken. To work around that, we set HISTORY via the environment
    #   instead of using -f.
    history_file=$($condor_binpath/condor_config_val $target -schedd history)
    if [ "$?" == "0" ]; then
	line=$(echo $FORMAT | _condor_HISTORY="$history_file" xargs $condor_binpath/condor_history -backwards -match 1 $id)
	if  [ ! -z "$line" ] ; then
	    echo "0$(make_ad $job "$line")"
	    exit 0
	fi
    fi

    # If we still do not have a result it is possible that a race
    # condition masked the status, in which case we want to directly
    # query the Schedd to make absolutely sure there is no status to
    # be found.
    line=$(echo $FORMAT | xargs $condor_binpath/condor_q $target $id)
    if  [ -z "$line" ] ; then
	echo " 1 Status\\ not\\ found"
	exit 1
    else
	echo "0$(make_ad $job "$line")"
	exit 0
    fi

done
