/*
# Copyright (c) Members of the EGEE Collaboration. 2009. 
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

#define CSTR_CHARS         25

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

int IntStateQuery();
int FinalStateQuery(char *query);
int AssignFinalState(char *batchid);
int usage();
int short_usage();

int runfinal=FALSE;
char *command_string;
char *sge_helper_path="/opt/glite/bin/sge_helper";
char *sge_root="/usr/local/sge";
char *sge_cell="default";
char *registry_file;
int purge_interval=864000;
int finalstate_query_interval=5;
int alldone_interval=864000;
int debug=0;
int nodmn=0;
char *sge_batch_caching_enabled="Not";
char *batch_command_caching_filter=NULL;
char *batch_command=NULL;

FILE *debuglogfile;
char *debuglogname=NULL;

job_registry_handle *rha;
config_handle *cha;
config_entry *ret;
char *progname="BUpdaterSGE";

