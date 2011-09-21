/*
#  File:     BNotifier.c
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

#include "BNotifier.h"

int
main(int argc, char *argv[])
{

	int set = 1;
	int status;
	int version = 0;
	int list_c;
	char ainfo_port_string[16];
	struct addrinfo ai_req, *ai_ans, *cur_ans;
	int address_found;

	pthread_t CreamThd;
	pthread_t PollThd;
	config_handle *cha;
	config_entry *ret;
	char *pidfile=NULL;
	
	int c;				

        static int help;
        static int short_help;
	
	int loop_interval=DEFAULT_LOOP_INTERVAL;

	while (1) {
		static struct option long_options[] =
		{
		{"help",      no_argument,     &help,       1},
		{"usage",     no_argument,     &short_help, 1},
		{"nodaemon",  no_argument,       0, 'o'},
		{"version",   no_argument,       0, 'v'},
		{0, 0, 0, 0}
		};

		int option_index = 0;
     
		c = getopt_long (argc, argv, "vo",long_options, &option_index);
     
		if (c == -1){
			break;
		}
     
		switch (c)
		{

		case 0:
		if (long_options[option_index].flag != 0){
			break;
		}
     
		case 'v':
			version=1;
			break;
	       
		case 'o':
			nodmn=1;
			break;

		case '?':
			break;
     
		default:
			abort ();
		}
	}
	
	if(help){
		usage();
	}
	 
	if(short_help){
		short_usage();
	}

	argv0 = argv[0];
	
	/*Ignore sigpipe*/
    
	signal(SIGPIPE, SIG_IGN);             
        signal(SIGHUP,sighup);
 
	if(version) {
		printf("%s Version: %s\n",progname,VERSION);
		exit(EXIT_SUCCESS);
	}   

	/* Checking configuration */
	check_config_file("NOTIFIER");

	/* Reading configuration */
	cha = config_read(NULL);
	if (cha == NULL)
	{
		fprintf(stderr,"Error reading config: ");
		perror("");
		exit(EXIT_FAILURE);
	}
	
	ret = config_get("bnotifier_debug_level",cha);
	if (ret != NULL){
		debug=atoi(ret->value);
	}
	
	ret = config_get("bnotifier_debug_logfile",cha);
	if (ret != NULL){
		debuglogname=strdup(ret->value);
                if(debuglogname == NULL){
                        sysfatal("strdup failed for debuglogname in main: %r");
                }
	}
	
	if(debug <=0){
		debug=0;
	}
    
	if(debuglogname){
		if((debuglogfile = fopen(debuglogname, "a+"))==0){
			debug = 0;
		}
	}else{
		debug = 0;
	}
        
	ret = config_get("job_registry",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key job_registry not found\n",argv0);
		sysfatal("job_registry not defined. Exiting");
	} else {
		registry_file=strdup(ret->value);
                if(registry_file == NULL){
                        sysfatal("strdup failed for registry_file in main: %r");
                }
	}
	
	ret = config_get("async_notification_port",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key async_notification_port not found\n",argv0);
	} else {
		async_notif_port =atoi(ret->value);
	}

	ret = config_get("bnotifier_loop_interval",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bnotifier_loop_interval not found using the default:%d\n",argv0,loop_interval);
	} else {
		loop_interval=atoi(ret->value);
	}
	
	ret = config_get("bnotifier_pidfile",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bnotifier_pidfile not found\n",argv0);
	} else {
		pidfile=strdup(ret->value);
                if(pidfile == NULL){
                        sysfatal("strdup failed for pidfile in main: %r");
                }
	}
	
	/* create listening socket for Cream */
    
	if ( !async_notif_port ) {
		sysfatal("Invalid port supplied for Cream: %r");
	}

	ai_req.ai_flags = AI_PASSIVE;
	ai_req.ai_family = PF_UNSPEC;
	ai_req.ai_socktype = SOCK_STREAM;
	ai_req.ai_protocol = 0; /* Any stream protocol is OK */

	sprintf(ainfo_port_string,"%5d",async_notif_port);

	if (getaddrinfo(NULL, ainfo_port_string, &ai_req, &ai_ans) != 0) {
		sysfatal("Error getting address of passive SOCK_STREAM socket: %r");
	}

	address_found = 0;
	for (cur_ans = ai_ans; cur_ans != NULL; cur_ans = cur_ans->ai_next) {

		if ((list_c = socket(cur_ans->ai_family,
				     cur_ans->ai_socktype,
				     cur_ans->ai_protocol)) == -1)
		{
			continue;
		}

		if(setsockopt(list_c, SOL_SOCKET, SO_REUSEADDR, &set, sizeof(set)) < 0) 
		{
			close(list_c);
			syserror("setsockopt() failed: %r");
		}
		if (bind(list_c,cur_ans->ai_addr, cur_ans->ai_addrlen) == 0) 
		{
			address_found = 1;
			break;
		}
		close(list_c);
	}
	freeaddrinfo(ai_ans);

	if ( address_found == 0 ) {
		sysfatal("Error creating and binding socket: %r");
	}

	if ( listen(list_c, LISTENQ) < 0 ) {
		sysfatal("Error calling listen() in main: %r");
	}
    
	if( !nodmn ) daemonize();
	
	if( pidfile ){
		writepid(pidfile);
		free(pidfile);
	}
       
	config_free(cha);

	pthread_create(&CreamThd, NULL, (void *(*)(void *))CreamConnection, (void *)list_c);
	pthread_create(&PollThd, NULL, (void *(*)(void *))PollDB, (void *)NULL);

	pthread_join(PollThd, (void **)&status);
	pthread_exit(NULL);
 
}

/*---Functions---*/

int
PollDB()
{
        FILE *fd;
        job_registry_entry *en;
	job_registry_handle *rha;
	job_registry_handle *rhc;
	char *buffer=NULL;
	char *finalbuffer=NULL;
        char *cdate=NULL;
	time_t now;
        int  maxtok,i,maxtokl;
        char **tbuf;
        char **lbuf;
	int len=0,flen=0;
        struct stat sbuf;
        int rc;
	char *regfile;
        char *cp=NULL;
	
	rha=job_registry_init(registry_file, BY_BATCH_ID);
	if (rha == NULL){
		do_log(debuglogfile, debug, 1, "%s: Error initialising job registry %s\n",argv0,registry_file);
		fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
		perror("");
	}
	
	for(;;){
	
		now=time(NULL);
	
		if(!startnotify && !startnotifyjob && !(firstnotify && sentendonce)){
			sleep(loop_interval);
			continue;
		}

		if(startnotify){

                	regfile=make_message("%s/registry",registry_file);
        		rc=stat(regfile,&sbuf);
			free(regfile);
			if(sbuf.st_mtime<lastnotiftime){
				do_log(debuglogfile, debug, 3, "Skip registry opening: mtime:%d lastn:%d\n",sbuf.st_mtime,lastnotiftime);
				sleep(loop_interval);
				continue;
			}
			do_log(debuglogfile, debug, 3, "Normal registry opening: mtime:%d lastn:%d\n",sbuf.st_mtime,lastnotiftime);

			fd = job_registry_open(rha, "r");
			if (fd == NULL)
			{
				do_log(debuglogfile, debug, 1, "%s: Error opening job registry %s\n",argv0,registry_file);
				fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
				perror("");
				sleep(loop_interval);
				continue;
			}
			if (job_registry_rdlock(rha, fd) < 0)
			{
				do_log(debuglogfile, debug, 1, "%s: Error read locking registry %s\n",argv0,registry_file);
				fprintf(stderr,"%s: Error read locking registry %s :",argv0,registry_file);
				perror("");
				sleep(loop_interval);
				continue;
			}
			while ((en = job_registry_get_next(rha, fd)) != NULL)
			{
		
				if(en->mdate >= lastnotiftime && en->mdate < now && en->user_prefix && strstr(en->user_prefix,creamfilter)!=NULL && strlen(en->updater_info)>0)
				{
					buffer=ComposeClassad(en);
					len=strlen(buffer);
					if(finalbuffer != NULL){
						flen=strlen(finalbuffer);
					}else{
						flen=0;
					}
					finalbuffer = realloc(finalbuffer,flen+len+2);
					if (finalbuffer == NULL){
						sysfatal("can't realloc finalbuffer in PollDB: %r");
					}
					if(flen==0){
						finalbuffer[0]='\000';
					}
					strcat(finalbuffer,buffer);
					free(buffer);
				}
				free(en);
			}

			if(finalbuffer != NULL){
				if(NotifyCream(finalbuffer)!=-1){
	        			/* change last notification time */
					lastnotiftime=now;
				}
				free(finalbuffer);
				finalbuffer=NULL;
			}
			
			fclose(fd);
			
			
		}else if(startnotifyjob){
			rhc=job_registry_init(registry_file, BY_USER_PREFIX);
			if (rhc == NULL){
				do_log(debuglogfile, debug, 1, "%s: Error initialising job registry %s\n",argv0,registry_file);
				fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
				perror("");
			}
			do_log(debuglogfile, debug, 2, "%s:Job list for notification:%s\n",argv0,joblist_string);
			maxtok=strtoken(joblist_string,',',&tbuf);
   			for(i=0;i<maxtok;i++){
        			if ((en=job_registry_get(rhc, tbuf[i])) != NULL){
					buffer=ComposeClassad(en);
				}else{
					cdate=iepoch2str(now);
					maxtokl=strtoken(tbuf[i],'_',&lbuf);
					if(lbuf[1]){
						if ((cp = strrchr (lbuf[1], '\n')) != NULL){
							*cp = '\0';
						}
						if ((cp = strrchr (lbuf[1], '\r')) != NULL){
							*cp = '\0';
						}
						buffer=make_message("[BlahJobName=\"%s\"; ClientJobId=\"%s\"; JobStatus=4; JwExitCode=999; ExitReason=\"BUpdater is not able to find the job anymore\"; Reason=\"BUpdater is not able to find the job anymore\"; ChangeTime=\"%s\"; ]\n",tbuf[i],lbuf[1],cdate);
					}
					freetoken(&lbuf,maxtokl);
					free(cdate);
				}
				free(en);
				len=strlen(buffer);
				if(finalbuffer != NULL){
					flen=strlen(finalbuffer);
				}else{
					flen=0;
				}
				finalbuffer = realloc(finalbuffer,flen+len+2);
				if (finalbuffer == NULL){
					sysfatal("can't realloc finalbuffer in PollDB: %r");
				}
				if(flen==0){
					finalbuffer[0]='\000';
				}
				strcat(finalbuffer,buffer);
				free(buffer);
			}
			freetoken(&tbuf,maxtok);
			
			if(finalbuffer != NULL){
				if(NotifyCream(finalbuffer)!=-1){
	        			/* change last notification time */
					lastnotiftime=now;
					startnotifyjob=FALSE;
				}
				free(finalbuffer);
				finalbuffer=NULL;
			}
			job_registry_destroy(rhc);
		}

		if(firstnotify && sentendonce){
			if(NotifyCream("NTFDATE/END\n")!=-1){
				startnotify=TRUE;
				sentendonce=FALSE;
				firstnotify=FALSE;
			}
		}		
		sleep(loop_interval);
	}
                
	job_registry_destroy(rha);
	
	return 0;
}

char *
ComposeClassad(job_registry_entry *en)
{

	char *strudate=NULL;
	char *buffer=NULL;
	char *wn=NULL;
	char *excode=NULL;
	char *exreas=NULL;
	char *blahid=NULL;
	char *clientid=NULL;
        int  maxtok;
        char **tbuf;
	char *cp=NULL;
			
	if((buffer=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc buffer in PollDB: %r");
	}
		
	strudate=iepoch2str(en->udate);
	sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%d; ChangeTime=\"%s\";",en->batch_id, en->status, strudate);
	free(strudate);

	if (strlen(en->wn_addr) > 0){
		wn=make_message(" WorkerNode=\"%s\";",en->wn_addr);
		strcat(buffer,wn);
		free(wn);
		}
	if (en->status == 3 || en->status == 4){
		excode=make_message(" JwExitCode=%d; Reason=\"reason=%d\";", en->exitcode, en->exitcode);
		strcat(buffer,excode);
		free(excode);
	}
	if (strlen(en->exitreason) > 0){
		exreas=make_message(" ExitReason=\"%s\";", en->exitreason);
		strcat(buffer,exreas);
		free(exreas);
	}
	if (strlen(en->user_prefix) > 0){
		maxtok=strtoken(en->user_prefix,'_',&tbuf);
		if(tbuf[1]){
			if ((cp = strrchr (tbuf[1], '\n')) != NULL){
				*cp = '\0';
			}
			if ((cp = strrchr (tbuf[1], '\r')) != NULL){
				*cp = '\0';
			}
			 clientid=make_message(" ClientJobId=\"%s\";",tbuf[1]);
		}
		blahid=make_message("%s BlahJobName=\"%s\";",clientid, en->user_prefix);
		strcat(buffer,blahid);
		free(blahid);
		freetoken(&tbuf,maxtok);
		free(clientid);
	}
	strcat(buffer,"]\n");
		
	return buffer;
		
}

void 
CreamConnection(int c_sock)
{ 
/*
startnotify 	controls the normal operation in PollDB
startnotifyjob 	is used to send notification of jobs contained in joblist_string 
firstnotify 	controls if NTFDATE/END has to be sent (together with sentendonce)
sentendonce 	controls if NTFDATE/END has to be sent (is used to permit STARTNOTIFYJOBLIST to be used during
            	normal notifier operation without sending NTFDATE/END). It is reset to TRUE only by CREAMFILTER command
            	otherwise it remains FALSE after the first notification (finished with NTFDATE/END).
creamisconn 	starts all the normal usage: without it no notifications are sent to cream

So the initial commands should be:
CREAMFILTER
PARSERVERSION

STARTNOTIFY 
or 
STARTNOTIFYJOBLIST
STARTNOTIFYJOBEND

during the normal usage to have info about a list of job:
STARTNOTIFYJOBLIST
STARTNOTIFYJOBEND

*/

	char      *buffer;
	time_t    now;

	if((buffer=calloc(LISTBUFFER,1)) == 0){
		sysfatal("can't malloc buffer in CreamConnection: %r");
	}

	while ( 1 ) {
	
		do_log(debuglogfile, debug, 1, "Listening for new connection in CreamConnection\n");
		if ( (conn_c = accept(c_sock, NULL, NULL) ) < 0 ) {
			do_log(debuglogfile, debug, 1, "Fatal Error:Error calling accept() on c_sock in CreamConnection\n");
			sysfatal("Error calling accept() in CreamConnection: %r");
		}
		while ( 1 ) {
			*buffer = 0;
			if(Readline(conn_c, buffer, LISTBUFFER-1)<=0){
				close(conn_c);
				creamisconn=FALSE;
				break;
			}

			if(strlen(buffer)>0){
				do_log(debuglogfile, debug, 1, "Received for Cream:%s\n",buffer);
				if(buffer && strstr(buffer,"STARTNOTIFY/")!=NULL){
					NotifyStart(buffer);
					startnotify=TRUE;
					firstnotify=TRUE;
				} else if(buffer && strstr(buffer,"STARTNOTIFYJOBLIST/")!=NULL){
					GetJobList(buffer);
					startnotifyjob=TRUE;
					startnotify=FALSE;
                               	} else if(buffer && strstr(buffer,"STARTNOTIFYJOBEND/")!=NULL){
					firstnotify=TRUE;
					now=time(NULL);
					lastnotiftime=now;
				} else if(buffer && strstr(buffer,"CREAMFILTER/")!=NULL){
                                        GetFilter(buffer);
					creamisconn=TRUE;
					sentendonce=TRUE;
				} else if(buffer && strstr(buffer,"PARSERVERSION/")!=NULL){
                                        GetVersion();
                                }
			}
		}
	} 
}

int 
GetVersion()
{

	char *out_buf;

	out_buf=make_message("%s__1\n",VERSION);
	Writeline(conn_c, out_buf, strlen(out_buf));
	do_log(debuglogfile, debug, 1, "Sent Reply for PARSERVERSION command:%s\n",out_buf);
	free(out_buf);
	
	return 0;
	
}

int 
GetFilter(char *buffer)
{

        int  maxtok;
        char **tbuf;
        char *cp=NULL;
        char * out_buf;

        maxtok=strtoken(buffer,'/',&tbuf);

        if(tbuf[1]){
		creamfilter=make_message("%s",tbuf[1]);
        	if(creamfilter == NULL){
                	sysfatal("strdup failed for creamfilter in GetFilter: %r");
        	}
                if ((cp = strrchr (creamfilter, '\n')) != NULL){
                	*cp = '\0';
                }
                if ((cp = strrchr (creamfilter, '\r')) != NULL){
                	*cp = '\0';
                }
		out_buf=make_message("CREAMFILTER set to %s\n",creamfilter);

        }else{
		out_buf=make_message("CREAMFILTER ERROR\n");
	}
		
	Writeline(conn_c, out_buf, strlen(out_buf));

	do_log(debuglogfile, debug, 1, "Sent Reply for CREAMFILTER command:%s\n",out_buf);

	freetoken(&tbuf,maxtok);
        free(out_buf);
	
        return 0;

}

int 
NotifyStart(char *buffer)
{

        int  maxtok;
        char **tbuf;
        char *cp=NULL;
	char *notifdate=NULL;
        time_t   notifepoch;
	
        maxtok=strtoken(buffer,'/',&tbuf);

        if(tbuf[1]){
                notifdate=strdup(tbuf[1]);
        	if(notifdate == NULL){
                	sysfatal("strdup failed for notifdate in NotifyStart: %r");
        	}
                if ((cp = strrchr (notifdate, '\n')) != NULL){
                        *cp = '\0';
                }
                if ((cp = strrchr (notifdate, '\r')) != NULL){
                        *cp = '\0';
                }
        }

	freetoken(&tbuf,maxtok);

	notifepoch=str2epoch(notifdate,"S");
	free(notifdate);

	lastnotiftime=notifepoch;
	
	return 0;

}

int 
GetJobList(char *buffer)
{

        int  maxtok;
        char **tbuf;
        char *cp=NULL;
	
        maxtok=strtoken(buffer,'/',&tbuf);

        if(tbuf[1]){
                joblist_string=strdup(tbuf[1]);
        	if(joblist_string == NULL){
                	sysfatal("strdup failed for joblist_string in GetJobList: %r");
        	}
                if ((cp = strrchr (joblist_string, '\n')) != NULL){
                        *cp = '\0';
                }
                if ((cp = strrchr (joblist_string, '\r')) != NULL){
                        *cp = '\0';
                }
        }

	freetoken(&tbuf,maxtok);

	return 0;

}


int
NotifyCream(char *buffer)
{

	int      retcod;
        
	struct   pollfd fds[2];
	struct   pollfd *pfds;
	int      nfds = 1;
    
	fds[0].fd = conn_c;
	fds[0].events = 0;
	fds[0].events = ( POLLOUT | POLLPRI | POLLERR | POLLHUP | POLLNVAL ) ;
	pfds = fds;    
    
	if(!creamisconn){
		return -1;
	}
    
	retcod = poll(pfds, nfds, bfunctions_poll_timeout); 
        
	if(retcod <0){
		close(conn_c);
		do_log(debuglogfile, debug, 1, "Fatal Error:Poll error in NotifyCream errno:%d\n",errno);
		sysfatal("Poll error in NotifyCream: %r");
	}else if ( retcod == 0 ){
		do_log(debuglogfile, debug, 1, "Error:poll() timeout in NotifyCream\n");
		syserror("poll() timeout in NotifyCream: %r");
		return -1;
	}else if ( retcod > 0 ){
		if ( ( fds[0].revents & ( POLLERR | POLLNVAL | POLLHUP) )){
			switch (fds[0].revents){
			case POLLNVAL:
				do_log(debuglogfile, debug, 1, "Error:poll() file descriptor error in NotifyCream\n");
				syserror("poll() file descriptor error in NotifyCream: %r");
				return -1;
			case POLLHUP:
				do_log(debuglogfile, debug, 1, "Connection closed in NotifyCream\n");
				syserror("Connection closed in NotifyCream: %r");
				return -1;
			case POLLERR:
				do_log(debuglogfile, debug, 1, "Error:poll() POLLERR in NotifyCream\n");
				syserror("poll() POLLERR in NotifyCream: %r");
				return -1;
			}
		} else {
			
			Writeline(conn_c, buffer, strlen(buffer));
			do_log(debuglogfile, debug, 1, "Sent for Cream:%s",buffer);
		} 
	}       

	return 0;

}

void sighup()
{
        if(debug){
                fclose(debuglogfile);
                if((debuglogfile = fopen(debuglogname, "a+"))==0){
                        debug = 0;
                }
        }
}

int 
usage()
{
	printf("Usage: BNotifier [OPTION...]\n");
	printf("  -o, --nodaemon     do not run as daemon\n");
	printf("  -v, --version      print version and exit\n");
	printf("\n");
	printf("Help options:\n");
	printf("  -?, --help         Show this help message\n");
	printf("  --usage            Display brief usage message\n");
	exit(EXIT_SUCCESS);
}

int 
short_usage()
{
	printf("Usage: BNotifier [-ov?] [-o|--nodaemon] [-v|--version] [-?|--help] [--usage]\n");
	exit(EXIT_SUCCESS);
}
