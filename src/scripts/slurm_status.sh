#!/bin/bash

#  File:     pbs_status.sh
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#    18-Jun-2012: Original release
#
#  Description:
#    Return a classad describing the status of a SLURM job
#
# Copyright (c) Members of the EGEE Collaboration. 2012. 
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

. `dirname $0`/blah_load_config.sh

if [ "x$job_registry" != "x" ] ; then
   ${blah_sbin_directory}/blah_job_registry_lkup $@
   exit 0
else
   echo "job registry not enabled (required for SLURM support)" >&2
   exit 1
fi
