#!/bin/bash

# File:     sge_status.sh
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


[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

usage_string="Usage: $0 [-w] [-n]"

#get worker node info
getwn=""

#get creamport
getcreamport=""

###############################################################
# Parse parameters
###############################################################

while getopts "wn" arg 
do
    case "$arg" in
    w) getwn="--getworkernodes" ;;
    n) getcreamport="yes" ;;
    
    -) break ;;
    ?) echo $usage_string
       exit 1 ;;
    esac
done

shift `expr $OPTIND - 1`

if [ "x$getcreamport" == "xyes" ]
then
    exec `dirname $0`/blah_job_registry_lkup -n
fi

if [ -z "$sge_rootpath" ]; then sge_rootpath="/usr/local/sge/pro"; fi
if [ -r "$sge_rootpath/${sge_cellname:-default}/common/settings.sh" ]
then
  . $sge_rootpath/${sge_cellname:-default}/common/settings.sh
fi

tmpid=`echo "$@"|sed 's/.*\/.*\///g'`

# ASG Keith way
jobid=${tmpid}.default


blahp_status=`exec ${sge_helper_path:-/opt/glite/bin}/sge_helper --status $getwn $jobid`
retcode=$?

echo ${retcode}${blahp_status}
#exit $retcode
