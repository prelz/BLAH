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

#include <utime.h>
#include <sys/socket.h>
#include <arpa/inet.h> 
#include <pthread.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/poll.h>
#include <dirent.h>
#include "job_registry.h"
#include "Bfunctions.h"
#include "config.h"

#define LISTENQ            1024

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

/*  Function declarations  */

int PollDB();
int UpdateFileTime(int sec);
int NotifyStart(char *buffer);
int GetFilter(char *buffer);
int GetModTime(char *filename);
void CreamConnection(int c_sock);
int NotifyCream(char *buffer);

/* Variables initialization */

char *progname="BNotifier";

char *notiffile="/tmp/.notiftime.txt";

char *registry_file;

char *creamfilter=NULL;

int async_notif_port;

int debug=0;
int nodmn=0;

FILE *debuglogfile;
char *debuglogname;

struct sockaddr_in cservaddr;

int  list_c;
int  conn_c=-1;
int  c_sock;

int creamisconn=FALSE;
int startnotify=FALSE;
int firstnotify=FALSE;

