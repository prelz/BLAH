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
	
	poptContext poptcon;
	int rc=0;			     

	struct poptOption poptopt[] = {     
		{ "nodaemon",      'o', POPT_ARG_NONE,   &nodmn, 	    0, "do not run as daemon",    NULL },
		{ "version",       'v', POPT_ARG_NONE,   &version,	    0, "print version and exit",  NULL },
		POPT_AUTOHELP
		POPT_TABLEEND
	};
		int loop_interval=DEFAULT_LOOP_INTERVAL;

	argv0 = argv[0];
	
	/*Ignore sigpipe*/
    
	signal(SIGPIPE, SIG_IGN);             
        signal(SIGHUP,sighup);
    
	poptcon = poptGetContext(NULL, argc, (const char **) argv, poptopt, 0);
 
	if((rc = poptGetNextOpt(poptcon)) != -1){
		sysfatal("Invalid flag supplied: %r");
	}
	
	poptFreeContext(poptcon);
 
	if(version) {
		printf("%s Version: %s\n",progname,VERSION);
		exit(EXIT_SUCCESS);
	}   

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
                if(debug){
			fprintf(debuglogfile, "%s: key job_registry not found\n",argv0);
			fflush(debuglogfile);
		}
		sysfatal("job_registry not defined. Exiting");
	} else {
		registry_file=strdup(ret->value);
                if(registry_file == NULL){
                        sysfatal("strdup failed for registry_file in main: %r");
                }
	}
	
	ret = config_get("async_notification_port",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key async_notification_port not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		async_notif_port =atoi(ret->value);
	}

	ret = config_get("bnotifier_loop_interval",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key bnotifier_loop_interval not found using the default:%d\n",argv0,loop_interval);
			fflush(debuglogfile);
		}
	} else {
		loop_interval=atoi(ret->value);
	}
	
	ret = config_get("bnotifier_pidfile",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key bnotifier_pidfile not found\n",argv0);
			fflush(debuglogfile);
		}
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

	pthread_create(&CreamThd, NULL, (void *)CreamConnection, (void *)list_c);
	pthread_create(&PollThd, NULL, (void *)PollDB, (void *)NULL);

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
	char *new_finalbuffer;
        char *cdate=NULL;
	time_t now;
        int  maxtok,i;
        char **tbuf;
	int size;
	int len;
	
	rha=job_registry_init(registry_file, BY_BATCH_ID);
	if (rha == NULL){
		if(debug){
			fprintf(debuglogfile, "%s: Error initialising job registry %s",argv0,registry_file);
			fflush(debuglogfile);
		}
		fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
		perror("");
	}
	
	for(;;){
	
		now=time(NULL);
	
		if(!startnotify && !startnotifyjob){
			sleep(2);
			continue;
		}

		if(startnotify){
			fd = job_registry_open(rha, "r");
			if (fd == NULL)
			{
				if(debug){
					fprintf(debuglogfile, "%s: Error opening job registry %s",argv0,registry_file);
					fflush(debuglogfile);
				}
				fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
				perror("");
				sleep(loop_interval);
				continue;
			}
			if (job_registry_rdlock(rha, fd) < 0)
			{
				if(debug){
					fprintf(debuglogfile, "%s: Error read locking registry %s",argv0,registry_file);
					fflush(debuglogfile);
				}
				fprintf(stderr,"%s: Error read locking registry %s :",argv0,registry_file);
				perror("");
				sleep(loop_interval);
				continue;
			}
			while ((en = job_registry_get_next(rha, fd)) != NULL)
			{
		
				if(en->mdate >= GetModTime(notiffile) && en->mdate < now && en->user_prefix && strstr(en->user_prefix,creamfilter)!=NULL)
				{
					buffer=ComposeClassad(en);
					len=strlen(buffer);
					size+=len+2;
					new_finalbuffer = realloc(finalbuffer,size);
					if (new_finalbuffer == NULL)
					{
						fprintf(stderr,"%s: Out of memory.",argv0);
						continue;
					}
					if (finalbuffer == NULL) new_finalbuffer[0] = '\000';
					finalbuffer = new_finalbuffer;
					strcat(finalbuffer,buffer);
					free(buffer);
				}
				free(en);
			}

			if(finalbuffer != NULL){
				NotifyCream(finalbuffer);
				free(finalbuffer);
				finalbuffer=NULL;
			}
			
			fclose(fd);
			
	        	/* change date of notification file */
			UpdateFileTime(now);
			
		}else if(startnotifyjob){
			rhc=job_registry_init(registry_file, BY_USER_PREFIX);
			if (rhc == NULL){
				if(debug){
					fprintf(debuglogfile, "%s: Error initialising job registry %s",argv0,registry_file);
					fflush(debuglogfile);
				}
				fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
				perror("");
			}
			if(debug>1){
				fprintf(debuglogfile, "%s:Job list for notification:%s\n",argv0,joblist_string);
				fflush(debuglogfile);
			}
			maxtok=strtoken(joblist_string,',',&tbuf);
   			for(i=0;i<maxtok;i++){
        			if ((en=job_registry_get(rhc, tbuf[i])) != NULL){
					buffer=ComposeClassad(en);
					NotifyCream(buffer);
				}else{
					cdate=iepoch2str(now);
					buffer=make_message("[BlahJobName=\"%s\"; JobStatus=4; ExitCode=999; ExitReason=\"job not found\"; ChangeTime=\"%s\"; ]\n",tbuf[i],cdate);
					free(cdate);
					NotifyCream(buffer);
				}
				free(en);
				free(buffer);
			}
			freetoken(&tbuf,maxtok);
			
	        	/* change date of notification file */
			UpdateFileTime(now);
			startnotifyjob=FALSE;
			startnotify=TRUE;
			job_registry_destroy(rhc);
		}

		if(firstnotify && sentendonce){
			NotifyCream("NTFDATE/END\n");
			sentendonce=FALSE;
			firstnotify=FALSE;
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
		excode=make_message(" ExitCode=%d; Reason=\"reason=%d\";", en->exitcode, en->exitcode);
		strcat(buffer,excode);
		free(excode);
	}
	if (strlen(en->exitreason) > 0){
		exreas=make_message(" ExitReason=\"%s\";", en->exitreason);
		strcat(buffer,exreas);
		free(exreas);
	}
	if (strlen(en->user_prefix) > 0){
		if((clientid=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc clientid in ComposeClassad: %r");
		}
		maxtok=strtoken(en->user_prefix,'_',&tbuf);
		if(tbuf[1]){
			if ((cp = strrchr (tbuf[1], '\n')) != NULL){
				*cp = '\0';
			}
			if ((cp = strrchr (tbuf[1], '\r')) != NULL){
				*cp = '\0';
			}
			 sprintf(clientid," ClientJobId=\"%s\";",tbuf[1]);
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

int
UpdateFileTime(time_t sec)
{
	
	int fd;
	struct utimbuf utb;
	struct stat buf;
		
	fd = open(notiffile, O_RDWR | O_CREAT,S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
	if (fd < 0) {
		if(debug){
			fprintf(debuglogfile, "Error opening file in UpdateFileTime\n");
			fflush(debuglogfile);
		}
		fprintf(stderr,"Error opening file in UpdateFileTime: ");
		perror("");
		return 1;
	}
	close(fd);
	stat(notiffile,&buf);
	
	utb.actime = buf.st_atime;
	utb.modtime = sec;

	if (utime(notiffile, &utb)) {
		if(debug){
			fprintf(debuglogfile, "Error in utime in UpdateFileTime\n");
			fflush(debuglogfile);
		}
		fprintf(stderr,"Error in utime in UpdateFileTime: ");
		perror("");
		return 1;
	}
	
	return 0;

}

int
GetModTime(char *filename)
{
	struct stat buf;
	stat(filename,&buf);
	return buf.st_mtime;
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
	int       retcod;
    
	struct   pollfd fds[2];
	struct   pollfd *pfds;
	int      nfds = 1;
	int      timeout= 5000;
    
	fds[0].fd = c_sock;
	fds[0].events = 0;
	fds[0].events = ( POLLIN | POLLOUT | POLLPRI | POLLERR | POLLHUP | POLLNVAL ) ;
	pfds = fds;

	if((buffer=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc buffer in CreamConnection: %r");
	}

	while ( 1 ) {
	
		if(conn_c < 0){
	  
			retcod = poll(pfds, nfds, timeout);
		
			if(retcod <0){
				close(conn_c);
				if(debug){
					fprintf(debuglogfile, "Fatal Error:Poll error in CreamConnection\n");
					fflush(debuglogfile);
				}
				sysfatal("Poll error in CreamConnection: %r");
			}
    
			if ( retcod > 0 ){		
ret_c:		
				if ( ( fds[0].revents & ( POLLERR | POLLNVAL | POLLHUP) )){
					switch (fds[0].revents){
					case POLLNVAL:
						if(debug){
							fprintf(debuglogfile, "Error:poll() file descriptor error in CreamConnection\n");
							fflush(debuglogfile);
						}
						syserror("poll() file descriptor error in CreamConnection: %r");
						break;
					case POLLHUP:
						if(debug){
							fprintf(debuglogfile, "Error:Connection closed in CreamConnection\n");
							fflush(debuglogfile);
						}
						syserror("Connection closed in CreamConnection: %r");
						break;
					case POLLERR:
						if(debug){
							fprintf(debuglogfile, "Error:poll() POLLERR in CreamConnection\n");
							fflush(debuglogfile);
						}
						syserror("poll() POLLERR in CreamConnection: %r");
						break;
					}
				} else {
            
					if ( (conn_c = accept(c_sock, NULL, NULL) ) < 0 ) {
						if(debug){
							fprintf(debuglogfile, "Fatal Error:Error calling accept() in CreamConnection\n");
							fflush(debuglogfile);
						}
						sysfatal("Error calling accept() in CreamConnection: %r");
					}
					goto write_c;
				} 
			} 
		}else{
			retcod = poll(pfds, nfds, timeout);
			if( retcod < 0 ){
				close(conn_c);
				if(debug){
					fprintf(debuglogfile, "Fatal Error:Poll error in CreamConnection\n");
					fflush(debuglogfile);
				}
				sysfatal("Poll error in CreamConnection: %r");
			}
			if(retcod > 0 ){
				close(conn_c);
				goto ret_c;
			}
write_c:      
			buffer[0]='\0';
			Readline(conn_c, buffer, STR_CHARS-1);
			if(strlen(buffer)>0){
				if(debug){
					fprintf(debuglogfile, "Received for Cream:%s\n",buffer);
					fflush(debuglogfile);
				}
				if(buffer && strstr(buffer,"STARTNOTIFY/")!=NULL){
					NotifyStart(buffer);
					startnotify=TRUE;
					firstnotify=TRUE;
				} else if(buffer && strstr(buffer,"STARTNOTIFYJOBLIST/")!=NULL){
					GetJobList(buffer);
					startnotifyjob=TRUE;
                                        startnotify=FALSE;
                                } else if(buffer && strstr(buffer,"STARTNOTIFYJOBEND/")!=NULL){
                                        startnotify=TRUE;
					firstnotify=TRUE;
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
	if(debug){
		fprintf(debuglogfile, "Sent Reply for PARSERVERSION command:%s",out_buf);
		fflush(debuglogfile);
	}
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

        if((out_buf=calloc(STR_CHARS,1)) == 0){
                sysfatal("can't malloc out_buf: %r");
        }
	
        maxtok=strtoken(buffer,'/',&tbuf);

        if(tbuf[1]){
                creamfilter=strdup(tbuf[1]);
        	if(creamfilter == NULL){
                	sysfatal("strdup failed for creamfilter in GetFilter: %r");
        	}
                if ((cp = strrchr (creamfilter, '\n')) != NULL){
                	*cp = '\0';
                }
                if ((cp = strrchr (creamfilter, '\r')) != NULL){
                	*cp = '\0';
                }
		sprintf(out_buf,"CREAMFILTER set to %s\n",creamfilter);

        }else{
		sprintf(out_buf,"CREAMFILTER ERROR\n");
	}
		
	Writeline(conn_c, out_buf, strlen(out_buf));

	if(debug){
		fprintf(debuglogfile, "Sent Reply for CREAMFILTER command:%s",out_buf);
		fflush(debuglogfile);
	}

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

	UpdateFileTime(notifepoch);
	
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
	int      timeout= 1000000;
    
	time_t   now;
	char     *nowtm;
	char     *cp;
    
	fds[0].fd = conn_c;
	fds[0].events = 0;
	fds[0].events = ( POLLIN | POLLOUT | POLLPRI | POLLERR | POLLHUP | POLLNVAL ) ;
	pfds = fds;    
    
	if(!creamisconn){
		free(buffer);
		return -1;
	}
    
    
	retcod = poll(pfds, nfds, timeout); 
        
	if(retcod <0){
		close(conn_c);
		if(debug){
			fprintf(debuglogfile, "Fatal Error:Poll error in NotifyCream errno:%d\n",errno);
			fflush(debuglogfile);
		}
		sysfatal("Poll error in NotifyCream: %r");
	}
    
	if ( retcod > 0 ){
		if ( ( fds[0].revents & ( POLLERR | POLLNVAL | POLLHUP) )){
			switch (fds[0].revents){
			case POLLNVAL:
				if(debug){
					fprintf(debuglogfile, "Error:poll() file descriptor error in NotifyCream\n");
					fflush(debuglogfile);
				}
				syserror("poll() file descriptor error in NotifyCream: %r");
				break;
			case POLLHUP:
				if(debug){
					fprintf(debuglogfile, "Connection closed in NotifyCream\n");
					fflush(debuglogfile);
				}
				syserror("Connection closed in NotifyCream: %r");
				break;
			case POLLERR:
				if(debug){
					fprintf(debuglogfile, "Error:poll() POLLERR in NotifyCream\n");
					fflush(debuglogfile);
				}
				syserror("poll() POLLERR in NotifyCream: %r");
				break;
			}
		} else {
			now=time(NULL);
			nowtm=ctime(&now);
			if ((cp = strrchr (nowtm, '\n')) != NULL){
				*cp = '\0';
			}
			if ((cp = strrchr (nowtm, '\r')) != NULL){
				*cp = '\0';
			}
			
			Writeline(conn_c, buffer, strlen(buffer));
			if(debug){
				fprintf(debuglogfile, "%s Sent for Cream:%s",nowtm,buffer);
				fflush(debuglogfile);
			}
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
