/*
#  File:     BNotifier.h
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

#define LISTENQ            1024
#define LISTBUFFER         5000000

#define DEFAULT_LOOP_INTERVAL 5

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

/*  Function declarations  */

int PollDB();
char *ComposeClassad(job_registry_entry *en);
int NotifyStart(char *buffer);
int GetVersion();
int GetFilter(char *buffer);
int GetJobList(char *buffer);
void CreamConnection(int c_sock);
int NotifyCream(char *buffer);
void sighup();
int usage();
int short_usage();

/* Variables initialization */

char *progname="BNotifier";

char *registry_file;

char *creamfilter="";

int async_notif_port;

int debug=FALSE;
int nodmn=FALSE;

FILE *debuglogfile;
char *debuglogname;

int  conn_c=-1;
int  c_sock;

int creamisconn=FALSE;
int startnotify=FALSE;
int startnotifyjob=FALSE;
int firstnotify=FALSE;
int sentendonce=FALSE;

char *joblist_string="";

int loop_interval=DEFAULT_LOOP_INTERVAL;

time_t lastnotiftime;
