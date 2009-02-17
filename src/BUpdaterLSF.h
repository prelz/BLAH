/*
#  File:     BUpdaterLSF.h
#
#  Author:   Massimo Mezzadri
#  e-mail:   Massimo.Mezzadri@mi.infn.it
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
*/

#include "job_registry.h"
#include "Bfunctions.h"
#include "config.h"

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

int IntStateQueryShort();
int IntStateQuery();
int FinalStateQuery(time_t start_date);
int AssignFinalState(char *batchid);
int get_susp_timestamp(char *jobid);
int get_resume_timestamp(char *jobid);

int runfinal=FALSE;
char *lsf_binpath;
char *registry_file;
int purge_interval=864000;
int finalstate_query_interval=30;
int alldone_interval=600;
int bhist_logs_to_read=1;
char *bjobs_long_format="yes";
char *use_bhist_for_susp="no";
int debug=0;
int nodmn=0;

bupdater_active_jobs bact;

FILE *debuglogfile;
char *debuglogname;

job_registry_handle *rha;
config_handle *cha;
config_entry *ret;
char *progname="BUpdaterLSF";

