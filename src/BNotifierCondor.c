#include "BNotifierCondor.h"

int
main(int argc, char *argv[])
{

	int       set = 1;
	int       status;
	int       version = 0;
	int       list_c;

	pthread_t CreamThd;
	pthread_t PollThd;
	config_handle *cha;
	config_entry *ret;
	char *path=NULL;
	char *pidfile=NULL;
	
	poptContext poptcon;
	int rc=0;			     

	struct poptOption poptopt[] = {     
		{ "nodaemon",      'o', POPT_ARG_NONE,   &nodmn, 	    0, "do not run as daemon",    NULL },
		{ "version",       'v', POPT_ARG_NONE,   &version,	    0, "print version and exit",  NULL },
		POPT_AUTOHELP
		POPT_TABLEEND
	};
	
	argv0 = argv[0];
	
	/*Ignore sigpipe*/
    
	signal(SIGPIPE, SIG_IGN);             
    
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
	path=getenv("BLAHPD_CONFIG_LOCATION");
	cha = config_read(NULL);
	if (cha == NULL)
	{
		fprintf(stderr,"Error reading config from %s: ",path);
		perror("");
		exit(EXIT_FAILURE);
	}
	
	ret = config_get("debug_level",cha);
	if (ret != NULL){
		debug=atoi(ret->value);
	}
	
	ret = config_get("debug_logfile",cha);
	if (ret != NULL){
		debuglogname=strdup(ret->value);
	}
	
	if(debug <=0){
		debug=0;
	}
    
	if(debug){
		if((debuglogfile = fopen(debuglogname, "a+"))==0){
			debuglogfile =  fopen("/dev/null", "a+");
		}
	}
        
	ret = config_get("job_registry",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key job_registry not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		registry_file=strdup(ret->value);
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

	ret = config_get("bnotifier_pidfile",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key bnotifier_pidfile not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		pidfile=strdup(ret->value);
	}
	
	/* create listening socket for Cream */
    
	if ( !async_notif_port ) {
		sysfatal("Invalid port supplied for Cream: %r");
	}

	if ( (list_c = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
		sysfatal("Error creating listening socket in main: %r");
	}

	if(setsockopt(list_c, SOL_SOCKET, SO_REUSEADDR, &set, sizeof(set)) < 0) {
		syserror("setsockopt() failed: %r");
	}

	memset(&cservaddr, 0, sizeof(cservaddr));
	cservaddr.sin_family	= AF_INET;
	cservaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	cservaddr.sin_port	= htons(async_notif_port);

	if ( bind(list_c, (struct sockaddr *) &cservaddr, sizeof(cservaddr)) < 0 ) {
		sysfatal("Error calling bind() in main: %r");
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
	char *buffer;
	char *wn;
	char *excode;
	char *exreas;
	char *blahid;
	char *strudate;
	time_t now;
	
	while(1){
	
		now=time(NULL);
	
		if(!creamisconn){
			sleep(2);
			continue;
		}
		
		rha=job_registry_init(registry_file, BY_BATCH_ID);
		if (rha == NULL)
                {
			if(debug){
				fprintf(debuglogfile, "%s: Error initialising job registry %s",argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
                        perror("");
			sleep(2);
                        continue;
                }
		fd = job_registry_open(rha, "r");
		if (fd == NULL)
		{
			if(debug){
				fprintf(debuglogfile, "%s: Error opening job registry %s",argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
			perror("");
			sleep(2);
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
			sleep(2);
			continue;
		}
		
		while ((en = job_registry_get_next(rha, fd)) != NULL)
		{
		
			if((buffer=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc buffer in PollDB: %r");
			}
		
			/* Compare en->mdate and modification time notiffile */
			if(en->mdate >= GetModTime(notiffile) && en->mdate < now && en->blah_id && strstr(en->blah_id,creamfilter)!=NULL)
			{
				strudate=iepoch2str(en->udate);
				sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%d; Timestamp=%s;",en->batch_id, en->status, strudate);
				free(strudate);

				if (strlen(en->wn_addr) > 0){
					if((wn=calloc(STR_CHARS,1)) == 0){
						sysfatal("can't malloc wn in PollDB: %r");
					}
					sprintf(wn," WorkerNode=\"%s\";",en->wn_addr);
					strcat(buffer,wn);
					free(wn);
				}
				if (en->exitcode > 0){
					if((excode=calloc(STR_CHARS,1)) == 0){
						sysfatal("can't malloc excode in PollDB: %r");
					}
					sprintf(excode," ExitCode=%d;", en->exitcode);
					strcat(buffer,excode);
					free(excode);
				}
				if (strlen(en->exitreason) > 0){
					if((exreas=calloc(STR_CHARS,1)) == 0){
						sysfatal("can't malloc exreas in PollDB: %r");
					}
					sprintf(exreas," ExitReason=\"%s\";", en->exitreason);
					strcat(buffer,exreas);
					free(exreas);
				}
				if (strlen(en->blah_id) > 0){
					if((blahid=calloc(STR_CHARS,1)) == 0){
						sysfatal("can't malloc blahid in PollDB: %r");
					}
					sprintf(blahid," BlahJobId=\"%s\";", en->blah_id);
					strcat(buffer,blahid);
					free(blahid);
				}
				strcat(buffer,"]\n");
				
				NotifyCream(buffer);
			}
			free(en);
			free(buffer);
		}

	        /* change date of notification file */
		UpdateFileTime(now);

		if(startnotify){
			NotifyCream("NTFDATE/END\n");
			startnotify=FALSE;
		}		
		fclose(fd);
                job_registry_destroy(rha);
		sleep(2);
	}
	return(0);
}
int
UpdateFileTime(int sec)
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
		return(1);
	}
	
	return(0);

}

int
GetModTime(char *filename)
{
	struct stat buf;
	stat(filename,&buf);
	return(buf.st_mtime);
}

void 
CreamConnection(int c_sock)
{ 

	char      *buffer;
	int       retcod;
    
	struct   pollfd fds[2];
	struct   pollfd *pfds;
	int      nfds = 1;
	int      timeout= 5;
    
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
				if(buffer && strstr(buffer,"STARTNOTIFY")!=NULL){
					NotifyStart(buffer);
					creamisconn=TRUE;
					startnotify=TRUE;
				}
                                if(buffer && strstr(buffer,"CREAMFILTER")!=NULL){
                                        GetFilter(buffer);
                                }
			}
		}
	} 
}

int GetFilter(char *buffer){

        int  maxtok,i;
        char **tbuf;
        char *cp;

        if((tbuf=calloc(10 * sizeof *tbuf,1)) == 0){
                sysfatal("can't malloc tbuf: %r");
        }

        maxtok=strtoken(buffer,'/',tbuf);

        if(tbuf[1]){
                creamfilter=strdup(tbuf[1]);
                if ((cp = strrchr (creamfilter, '\n')) != NULL){
                        *cp = '\0';
                }
        }

        for(i=0;i<maxtok;i++){
                free(tbuf[i]);
        }
        free(tbuf);

        return(0);

}

int NotifyStart(char *buffer){

        int  maxtok,i;
        char **tbuf;
        char *cp;
	char *notifdate;
        int   notifepoch;
	
        if((tbuf=calloc(10 * sizeof *tbuf,1)) == 0){
                sysfatal("can't malloc tbuf: %r");
        }

        maxtok=strtoken(buffer,'/',tbuf);

        if(tbuf[1]){
                notifdate=strdup(tbuf[1]);
                if ((cp = strrchr (notifdate, '\n')) != NULL){
                        *cp = '\0';
                }
        }

        for(i=0;i<maxtok;i++){
                free(tbuf[i]);
        }
        free(tbuf);

	notifepoch=str2epoch(notifdate,"S");
	free(notifdate);
	UpdateFileTime(notifepoch);
	
	return(0);

}


int
NotifyCream(char *buffer)
{

	int      retcod;
        
	struct   pollfd fds[2];
	struct   pollfd *pfds;
	int      nfds = 1;
	int      timeout= 1;
    
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
			fprintf(debuglogfile, "Fatal Error:Poll error in NotifyCream\n");
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
			
			Writeline(conn_c, buffer, strlen(buffer));
			if(debug){
				fprintf(debuglogfile, "%s Sent for Cream:%s",nowtm,buffer);
				fflush(debuglogfile);
			}
		} 
	}       

	return(0);

}

