#include "BLParserLSF.h"

int main(int argc, char *argv[]) {

    struct    sockaddr_in servaddr;  /*  socket address structure  */
    int       set = 1;
    int       i,j;
    int       status;
    int       list_s;
    int       list_c;
    FILE      *fpt;

    pthread_t ReadThd[NUMTHRDS];
    pthread_t UpdateThd;
    pthread_t CreamThd;

    argv0 = argv[0];
    
    /*Ignore sigpipe*/
    
    signal(SIGPIPE, SIG_IGN);             

   /* Get log dir name and port from conf file*/

    ldir=GetLogDir(argc,argv);
    
    if(dmn){
     daemonize();
    }
    
    if(debug){
     if((debuglogfile = fopen(debuglogname, "a+"))==0){
      debuglogfile =  fopen("/dev/null", "a+");
     }
    }
    
    if((eventsfile=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc eventsfile: %r");
    }
    eventsfile[0]='\0';
    
    strcat(eventsfile,ldir);
    strcat(eventsfile,"/");
    strcat(eventsfile,lsbevents);
    
    free(ldir);

    /* test if logfile exists and is readable */
    
    if((fpt=fopen((char *)eventsfile, "r")) == 0){
     sysfatal("error opening %s: %r", eventsfile);
    }else{
     fclose(fpt);
    }
    
    /* Set to zero all the cache */
    
    for(j=0;j<RDXHASHSIZE;j++){
     rptr[j]=0;
    }
    for(j=0;j<CRMHASHSIZE;j++){
     nti[j]=0;
    }
    
    /*  Create the listening socket  */

    if ( (list_s = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
	fprintf(stderr, "%s: Error creating listening socket.\n",progname);
	exit(EXIT_FAILURE);
    }

    if(setsockopt(list_s, SOL_SOCKET, SO_REUSEADDR, &set, sizeof(set)) < 0) {
        fprintf(stderr,"%s: setsockopt() failed\n",progname);
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port        = htons(port);

    if ( bind(list_s, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0 ) {
	fprintf(stderr, "%s: Error calling bind() in main\n",progname);
	exit(EXIT_FAILURE);
    }
    
    if ( listen(list_s, LISTENQ) < 0 ) {
    	fprintf(stderr, "%s: Error calling listen()\n",progname);
    	exit(EXIT_FAILURE);
    }
    
    /* create listening socket for Cream */
    
    if(usecream>0){
      
      if ( !creamport ) {
     	 fprintf(stderr, "%s: Invalid port supplied for Cream\n",progname);
     	 exit(EXIT_FAILURE);
      }

      if ( (list_c = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
   	  fprintf(stderr, "%s: Error creating listening socket.\n",progname);
   	  exit(EXIT_FAILURE);
      }

      if(setsockopt(list_c, SOL_SOCKET, SO_REUSEADDR, &set, sizeof(set)) < 0) {
     	  fprintf(stderr,"%s: setsockopt() failed\n",progname);
      }

      memset(&cservaddr, 0, sizeof(cservaddr));
      cservaddr.sin_family	= AF_INET;
      cservaddr.sin_addr.s_addr = htonl(INADDR_ANY);
      cservaddr.sin_port	= htons(creamport);

      if ( bind(list_c, (struct sockaddr *) &cservaddr, sizeof(cservaddr)) < 0 ) {
   	  fprintf(stderr, "%s: Error calling bind() in main\n",progname);
   	  exit(EXIT_FAILURE);
      }
      
      if ( listen(list_c, LISTENQ) < 0 ) {
     	  fprintf(stderr, "%s: Error calling listen()\n",progname);
     	  exit(EXIT_FAILURE);
      }
    }
    
    for(i=0;i<NUMTHRDS;i++){
     pthread_create(&ReadThd[i], NULL, LookupAndSend, (void *)list_s);
    }
    
    if(usecream>0){
     pthread_create(&CreamThd, NULL, CreamConnection, (void *)list_c);
    }
    pthread_create(&UpdateThd, NULL, mytail, (void *)eventsfile);
    pthread_join(UpdateThd, (void **)&status);
    
    pthread_exit(NULL);
 
}

/*---functions---*/

ssize_t Readline(int sockd, void *vptr, size_t maxlen) {
    ssize_t n, rc;
    char    c, *buffer;

    buffer = vptr;

    for ( n = 1; n < maxlen; n++ ) {
	
	if ( (rc = read(sockd, &c, 1)) == 1 ) {
	    *buffer++ = c;
	    if ( c == '\n' )
		break;
	}
	else if ( rc == 0 ) {
	    if ( n == 1 )
		return 0;
	    else
		break;
	}
	else {
	    if ( errno == EINTR )
		continue;
	    return -1;
	}
    }

    *buffer = 0;
    return n;
}

ssize_t Writeline(int sockd, const void *vptr, size_t n) {
    size_t      nleft;
    ssize_t     nwritten;
    const char *buffer;

    buffer = vptr;
    nleft  = n;

     while ( nleft > 0 ) {

 	if ( (nwritten = write(sockd, (char *)vptr, nleft)) <= 0 ) {
            if ( errno == EINTR ) {
		nwritten = 0;
	    }else{
		return -1;
	    }
	}
	nleft  -= nwritten;
	buffer += nwritten;
    }

    return n;
}

unsigned hash(char *s){

 unsigned hashval;

 for(hashval = 0; *s!='\0';s++){
  hashval = *s + 31 *hashval;
 }
 return hashval % RDXHASHSIZE;
}


void *mytail (void *infile){    
        
    char *linebuffer;
    
    if((linebuffer=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc linebuffer: %r");
    }
    
    follow((char *)infile, linebuffer);
   
   return 0;
}

void follow(char *infile, char *line){
    FILE *fp;
    long off = 0;
    long old_off = 0;
    long real_off = 0;

    for(;;){
        if((fp=fopen((char *)infile, "r")) == 0){
         syserror("error opening %s: %r", infile);
	 sleep(1);
	 continue;
        }
        if(fseek(fp, off, SEEK_SET) < 0){
         sysfatal("couldn't seek: %r");
        }

        old_off=ftell(fp);
        if(fseek(fp, 0L, SEEK_END) < 0){
         sysfatal("couldn't seek: %r");
        }
        real_off=ftell(fp);

        if(real_off < old_off){
         off=0;
        }else{
         off=old_off;
        }
   
        if(fseek(fp, off, SEEK_SET) < 0){
          sysfatal("couldn't seek: %r");
         }
        
        off = tail(fp, line);
	fclose(fp);
	sleep(1);
    }        
}

long tail(FILE *fp, char *line){
    long off=0;

    while(fgets(line, STR_CHARS, fp)){
      if((strstr(line,rex_queued)!=NULL) || (strstr(line,rex_running)!=NULL) || (strstr(line,rex_status)!=NULL) || (strstr(line,rex_signal)!=NULL)){        
       if(debug >= 2){
	fprintf(debuglogfile, "Tail line:%s",line);
        fflush(debuglogfile);
       }
       AddToStruct(line,1);
      }
    }

    if((off=ftell(fp)) < 0){
        sysfatal("couldn't get file location: %r");
    }
    return off;
}


int InfoAdd(int id, char *value, const char * flag){

 if(debug){
  fprintf(debuglogfile, "Adding: ID:%d Type:%s Value:%s\n",rptr[id],flag,value);
  fflush(debuglogfile);
 } 
 /* set write lock */
 pthread_mutex_lock( &write_mutex );
 wlock=1;
 
 if((strcmp(flag,"JOBID")==0) && j2js[id] == NULL){
  
  j2js[id] = strdup("1");  
  j2bl[id] = strdup("\0");
  j2wn[id] = strdup("\0");
  j2ec[id] = strdup("\0");
  j2st[id] = strdup("\0");
  j2rt[id] = strdup("\0");
  j2ct[id] = strdup("\0");
  
 } else if((strcmp(flag,"JOBID")==0) && recycled==1){
 
  free(j2js[id]);
  free(j2bl[id]);
  free(j2wn[id]);
  free(j2ec[id]);
  free(j2st[id]);
  free(j2rt[id]);
  free(j2ct[id]);
  
  j2js[id] = strdup("1");  
  j2bl[id] = strdup("\0");
  j2wn[id] = strdup("\0");
  j2ec[id] = strdup("\0");
  j2st[id] = strdup("\0");
  j2rt[id] = strdup("\0");
  j2ct[id] = strdup("\0");
    
 } else if(strcmp(flag,"BLAHPNAME")==0){
 
  free(j2bl[id]);
  j2bl[id] = strdup(value);
  
 } else if(strcmp(flag,"WN")==0){
 
  free(j2wn[id]);
  j2wn[id] = strdup(value);
  
 } else if(strcmp(flag,"JOBSTATUS")==0){
 
  free(j2js[id]);
  j2js[id] = strdup(value);
  
 } else if(strcmp(flag,"EXITCODE")==0){

  free(j2ec[id]);
  j2ec[id] = strdup(value);
 
 } else if(strcmp(flag,"STARTTIME")==0){

  free(j2st[id]);
  j2st[id] = strdup(value);
 
 } else if(strcmp(flag,"RUNNINGTIME")==0){

  free(j2rt[id]);
  j2rt[id] = strdup(value);
 
 } else if(strcmp(flag,"COMPLTIME")==0){

  free(j2ct[id]);
  j2ct[id] = strdup(value);
 
 } else {
 
 /* release write lock */
   pthread_mutex_unlock( &write_mutex );
   wlock=0;

   return -1;
 
 }
   /* release write lock */
  pthread_mutex_unlock( &write_mutex );
  wlock=0;

  return 0;
}

int AddToStruct(char *line, int flag){

 /*if flag ==0 AddToStruct is called within GetOldLogs 
   if flag ==1 AddToStruct is called elsewhere*/

 int has_blah=0;
 unsigned h_blahjob;
 char *	rex;
 
 int id,realid;
 
 int  maxtok,ii; 
 char **tbuf;
 
 char *	jobid=NULL;
 char *	tj_time=NULL;
 char *	j_time=NULL;
 char *	tmptime=NULL;
 char *	j_status=NULL;
 char *	j_reason=NULL;
 char *	ex_status=NULL;
 char *	failex_status=NULL;
 char *	sig_status=NULL;
 char *	j_blahjob=NULL;
 char *	wnode=NULL;
 
 if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
 }

 maxtok=strtoken(line,' ',tbuf);
 
 if(maxtok>0){
  rex=strdup(tbuf[0]);
 }
 if(maxtok>2){
  tj_time=strdup(tbuf[2]);
  j_time=epoch2str(tj_time);
  tmptime=strdup(j_time);
 }
 if(maxtok>3){
  jobid=strdup(tbuf[3]);
  realid=atoi(jobid);
 }
 if(maxtok>4){
  j_status=strdup(tbuf[4]);
 }
 if(maxtok>5){
  j_reason=strdup(tbuf[5]);
 }
 if(maxtok>6){
  sig_status=strdup(tbuf[6]);
 }
 if(maxtok>9){
  wnode=strdup(tbuf[9]);
 }
 if(maxtok>10){
  ex_status=strdup(tbuf[10]);
 }
 if(maxtok>29){
  failex_status=strdup(tbuf[29]);
 }
 if(maxtok>41){
  j_blahjob=strdup(tbuf[41]);
  if((strstr(j_blahjob,blahjob_string)!=NULL) || (strstr(j_blahjob,cream_string)!=NULL)){
   has_blah=1;
  }
 }
 
 for(ii=0;ii<maxtok;ii++){
  free(tbuf[ii]);
 }
 free(tbuf);

 while (1){
  if(rcounter==0){
   break;
  }
  sleep(1);
 }
 
 id=UpdatePtr(realid);
 
 if(rex && (strcmp(rex,rex_queued)==0) && (has_blah)){

  InfoAdd(id,jobid,"JOBID");
  InfoAdd(id,j_time,"STARTTIME");
  InfoAdd(id,j_blahjob,"BLAHPNAME");
 
  h_blahjob=hash(j_blahjob);
  bjl[h_blahjob]=strdup(jobid);
  
  if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
   NotifyCream(id, "1", j2bl[id], "NA", "NA", j2st[id], flag);
  }
  
 } else if(j2bl[id] && ((strstr(j2bl[id],blahjob_string)!=NULL) || (strstr(j2bl[id],cream_string)!=NULL))){ 

  if(rex && strcmp(rex,rex_running)==0){

   InfoAdd(id,"2","JOBSTATUS");
   InfoAdd(id,wnode,"WN");
   InfoAdd(id,j_time,"RUNNINGTIME");
   
   if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
    NotifyCream(id, "2", j2bl[id], j2wn[id], "NA", j2rt[id], flag);
   }
  
  } else if(rex && strcmp(rex,rex_signal)==0){
  
   if(strstr(sig_status,"KILL")!=NULL){

    InfoAdd(id,"3","JOBSTATUS");

    if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
     if(j2wn[id]!=NULL){
      NotifyCream(id, "3", j2bl[id], j2wn[id], "NA", tmptime, flag);
     }else{
      NotifyCream(id, "3", j2bl[id], "NA", "NA", tmptime, flag);
     }
    }
   }
     
  } else if(rex && strcmp(rex,rex_status)==0){
    
   if(j_status && strcmp(j_status,"192")==0){

    InfoAdd(id,"4","JOBSTATUS");
    InfoAdd(id,"0","EXITCODE");
    InfoAdd(id,j_time,"COMPLTIME");
    
    if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
     NotifyCream(id, "4", j2bl[id], j2wn[id], j_reason, j2ct[id], flag);
    }

   }  else if(j_status && strcmp(j_status,"32")==0){

    if(j2js[id] && strcmp(j2js[id],"3")!=0){
     InfoAdd(id,"4","JOBSTATUS");
     InfoAdd(id,failex_status,"EXITCODE");
     InfoAdd(id,j_time,"COMPLTIME");
     if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
      NotifyCream(id, "4", j2bl[id], j2wn[id], failex_status, j2ct[id], flag);
     }
    }

   } else if((j_status && strcmp(j_status,"16")==0) || (j_status && strcmp(j_status,"8")==0) || (j_status && strcmp(j_status,"2")==0)){

    InfoAdd(id,"5","JOBSTATUS");
    
    if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
     if(j2wn[id]!=NULL){
      NotifyCream(id, "5", j2bl[id], j2wn[id], j_reason, tmptime, flag);
     }else{
      NotifyCream(id, "5", j2bl[id], "NA", j_reason, tmptime, flag);
     }
    }

   } else if(j_status && strcmp(j_status,"4")==0){

    InfoAdd(id,"2","JOBSTATUS");
    
    if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
     NotifyCream(id, "2", j2bl[id], j2wn[id], j_reason, tmptime, flag);
    }

   } else if(j_status && strcmp(j_status,"1")==0){

    InfoAdd(id,"1","JOBSTATUS");
    
    if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
     NotifyCream(id, "1", j2bl[id], "NA", j_reason, tmptime, flag);
    }

   }
  } /* closes if-else if on rex_ */
 } /* closes if-else if on jobid lookup */
 
 free(rex);
 free(j_time);
 free(tj_time);
 free(tmptime);
 free(jobid);
 free(j_status);
 free(sig_status);
 free(wnode);
 free(ex_status);
 free(failex_status);
 free(j_blahjob);

 return 0;
}

char *GetAllEvents(char *file){
 
 FILE *fp;
 char *line;
 char **opfile;
 int maxtok,i;

 if((opfile=malloc(STR_CHARS * sizeof *opfile)) == 0){
     sysfatal("can't malloc tbuf: %r");
 }
 
 maxtok = strtoken(file, ' ', opfile);

 if((line=malloc(STR_CHARS)) == 0){
  sysfatal("can't malloc line: %r");
 }
  
 for(i=0; i<maxtok; i++){ 
 
  if((fp=fopen(opfile[i], "r")) != 0){
   while(fgets(line, STR_CHARS, fp)){
    if((strstr(line,rex_queued)!=NULL) || (strstr(line,rex_running)!=NULL) || (strstr(line,rex_status)!=NULL) || (strstr(line,rex_signal)!=NULL)){
     AddToStruct(line,0);
    }
   }
  } else {
   fprintf(stderr, "%s: Cannot open %s file\n",progname,opfile[i]);
   exit(EXIT_FAILURE);
  }
  
  fclose(fp);
  free(opfile[i]);

 } /* close for*/
 free(file);
 free(line);
 free(opfile);
    
 return NULL;

}

void *LookupAndSend(int m_sock){ 
    
    char      *buffer;
    char      *out_buf;
    char      *logdate;
    char      *jobid;
    char      *h_jobid;
    char      *t_wnode;
    char      *pr_removal="Not";
    int       i,maxtok,ii;
    int       id;
    int       conn_s;
    char      **tbuf;
    char      *cp;
    
    while ( 1 ) {
	
	/*  Wait for a connection, then accept() it  */
	
	if ( (conn_s = accept(m_sock, NULL, NULL) ) < 0 ) {
	    fprintf(stderr, "%s: Error calling accept()\n",progname);
	    exit(EXIT_FAILURE);
	}

	if((buffer=malloc(STR_CHARS)) == 0){
	  sysfatal("can't malloc buffer in LookupAndSend: %r");
	}
        buffer[0]='\0';

	if((h_jobid=malloc(NUM_CHARS)) == 0){
	  sysfatal("can't malloc h_jobid in LookupAndSend: %r");
	}
        h_jobid[0]='\0';
	
	Readline(conn_s, buffer, STR_CHARS-1);	
	if(debug){
	 fprintf(debuglogfile, "Received:%s",buffer);
         fflush(debuglogfile);
	}
	
	/* printf("thread/0x%08lx\n",pthread_self()); */
	
	if((strlen(buffer)==0) || (strcmp(buffer,"\n")==0) || (strstr(buffer,"/")==0) || (strcmp(buffer,"/")==0)){
         
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
	 sprintf(out_buf,"\n");

	 goto close;
	}
	        
        if ((cp = strrchr (buffer, '\n')) != NULL){
         *cp = '\0';
        }

        if((tbuf=malloc(10 * sizeof *tbuf)) == 0){
          sysfatal("can't malloc tbuf: %r");
        }
	
        maxtok=strtoken(buffer,'/',tbuf);
        if(tbuf[0]){
         logdate=strdup(tbuf[0]);
        }else{
         if((logdate=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc buffer in LookupAndSend: %r");
         }
         logdate[0]='\0';
        }
        if(tbuf[1]){
         jobid=strdup(tbuf[1]);
        }else{
         if((jobid=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc buffer in LookupAndSend: %r");
         }
         jobid=strdup("\0");
        }

        for(ii=0;ii<maxtok;ii++){
         free(tbuf[ii]);
        }
        free(tbuf);
		

/* TEST reply */
       
        if(strcmp(logdate,"TEST")==0){
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
         sprintf(out_buf,"Y\n");
         goto close;
        }

/* VERSION reply */
       
        if(strcmp(logdate,"VERSION")==0){
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
         sprintf(out_buf,"%s\n",VERSION);
         goto close;
        }

/* get port where the parser is waiting for a connection from cream and send it to cream */
       
	if(strcmp(logdate,"CREAMPORT")==0){
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
	 sprintf(out_buf,"%d\n",creamport);
	 goto close;
	}
	
/* get jobid from blahjob id (needed by lsf_submit.sh) */
	
	if(strcmp(logdate,"BLAHJOB")==0){
         for(i=0;i<WRETRIES;i++){
	  if(wlock==0){
	   strcat(h_jobid,"\"");
	   strcat(h_jobid,jobid);
	   strcat(h_jobid,"\"");
	   if(bjl[hash(h_jobid)]==NULL){
	    sleep(1);
	    continue;
	   }
           if((out_buf=malloc(STR_CHARS)) == 0){
            sysfatal("can't malloc out_buf in LookupAndSend: %r");
           }
     	   sprintf(out_buf,"%s\n",bjl[hash(h_jobid)]);
	   goto close;
	  }else{
	   sleep(1);
	  } 
	 }
	 if(i==WRETRIES){

          if((out_buf=malloc(STR_CHARS)) == 0){
           sysfatal("can't malloc out_buf in LookupAndSend: %r");
          }
	  sprintf(out_buf,"\n");

	  goto close;
	 }
	}

        if((strlen(logdate)==0) || (strcmp(logdate,"\n")==0)){
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
         sprintf(out_buf,"\n");

         goto close;

        }
	
/* get all info from jobid */
     
        for(i=0;i<WRETRIES;i++){
	
	 if(wlock==0){
	 
          id=GetRdxId(atoi(jobid));

    	  if(id>0 && j2js[id]!=NULL){
	   
           if((out_buf=malloc(STR_CHARS)) == 0){
            sysfatal("can't malloc out_buf in LookupAndSend: %r");
           }
	   
	   if((t_wnode=malloc(STR_CHARS)) == 0){
	    sysfatal("can't malloc t_wnode in LookupAndSend: %r");
	   }
	   	   
           if(strcmp(j2wn[id],"\0")==0){
            t_wnode[0]='\0';
           }else{
            sprintf(t_wnode,"WorkerNode=%s;",j2wn[id]);
           }
           if((strcmp(j2js[id],"3")==0) || (strcmp(j2js[id],"4")==0)){
            pr_removal="Yes";
           } else {
            pr_removal="Not";
           }
           if(strcmp(j2js[id],"4")==0){
            sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\"; LRMSCompletedTime=\"%s\"; ExitCode=%s;]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], j2ct[id], j2ec[id], pr_removal);
           }else if(strcmp(j2rt[id],"\0")!=0){
            sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], pr_removal);
           }else{
            sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], pr_removal);
           }
	   free(t_wnode);
	  } else {
	  
     	   GetEventsInOldLogs(logdate);
	   
           id=GetRdxId(atoi(jobid));

     	   if(id>0 && j2js[id]!=NULL){

            if((out_buf=malloc(STR_CHARS)) == 0){
             sysfatal("can't malloc out_buf in LookupAndSend: %r");
            }
	    
	    if((t_wnode=malloc(STR_CHARS)) == 0){
	     sysfatal("can't malloc t_wnode in LookupAndSend: %r");
	    }
	   	   
            if(strcmp(j2wn[id],"\0")==0){
             t_wnode[0]='\0';
            }else{
             sprintf(t_wnode,"WorkerNode=%s;",j2wn[id]);
            }
            if((strcmp(j2js[id],"3")==0) || (strcmp(j2js[id],"4")==0)){
             pr_removal="Yes";
            } else {
             pr_removal="Not";
            }
            if(strcmp(j2js[id],"4")==0){
             sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\"; LRMSCompletedTime=\"%s\"; ExitCode=%s;]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], j2ct[id], j2ec[id], pr_removal);
            }else if(strcmp(j2rt[id],"\0")!=0){
             sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], pr_removal);
            }else{
             sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], pr_removal);
            }
	    free(t_wnode);
	     
	   } else {
            if((out_buf=malloc(STR_CHARS)) == 0){
             sysfatal("can't malloc out_buf in LookupAndSend: %r");
            }
     	    sprintf(out_buf,"JobId %s not found/Not\n",jobid);
    	   }
	   
     	  }
     	  break;
	 } 
	 else {
	  sleep(1);
	 }
	   
        }
	
	if(i==WRETRIES){
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
	 sprintf(out_buf,"Cache locked/Not\n");
	}
close:	
 	Writeline(conn_s, out_buf, strlen(out_buf));
	if(debug){
	 fprintf(debuglogfile, "Sent:%s",out_buf);
         fflush(debuglogfile);
	}
	
	free(out_buf);
	free(buffer);
        free(logdate);
        free(jobid);
        free(h_jobid);
	/*  Close the connected socket  */

	if ( close(conn_s) < 0 ) {
	    fprintf(stderr, "%s: Error calling close()\n",progname);
	    exit(EXIT_FAILURE);
	}
	
    } /* closes while */
    return(0); 
}

int GetEventsInOldLogs(char *logdate){

 char *loglist=NULL;
 
 loglist=GetLogList(logdate);
 
 if(loglist!=NULL){
  GetAllEvents(loglist);
 }
 
 return 0;
 
}

char *GetLogDir(int largc, char *largv[]){

 char *lsf_base_pathtmp;
 char *lsf_base_path;
 char *conffile;
 char *lsf_clustername;
 char *ls_out;
 char *logpath;
 char *line;
 char *command_string;
 int len;
 int version=0;
 
 struct stat     sbuf;
 
 char *ebinpath;
 char *econfpath;

 int  maxtok,ii; 
 char **tbuf;
 char *cp;
 char *s;
  
 FILE *fp;
 FILE *file_output;
 FILE *ls_output;
    
 const char *nport;

 poptContext poptcon;		     // popt's stuff	     
 int rc;			     
 struct poptOption poptopt[] = {     
     { "port",      'p', POPT_ARG_INT,    &port,	 0, "port",	       "<port number>"  },
     { "creamport", 'm', POPT_ARG_INT,    &creamport,	 0, "creamport",  "<creamport number>"  },
     { "binpath",   'b', POPT_ARG_STRING, &binpath,	 0, "LSF binpath",     "<LSFbinpath>"	},
     { "confpath",  'c', POPT_ARG_STRING, &confpath,	 0, "LSF confpath",    "<LSFconfpath>"  },
     { "logfile ",  'l', POPT_ARG_STRING, &debuglogname, 0, "DebugLogFile",    "<DebugLogFile>" },
     { "debug",     'd', POPT_ARG_INT,    &debug,	 0, "enable debugging", 	   NULL },
     { "daemon",    'D', POPT_ARG_NONE,   &dmn, 	 0, "run as daemon",		   NULL },
     { "version",   'v', POPT_ARG_NONE,   &version,	 0, "print version and exit",	   NULL },
     POPT_AUTOHELP
     POPT_TABLEEND
 };

 if((line=malloc(STR_CHARS)) == 0){
    sysfatal("can't malloc line: %r");
 }
 if((logpath=malloc(STR_CHARS)) == 0){
    sysfatal("can't malloc line: %r");
 }
 if((lsf_clustername=malloc(STR_CHARS)) == 0){
    sysfatal("can't malloc lsf_clustername: %r");
 }
 if((command_string=malloc(STR_CHARS)) == 0){
    sysfatal("can't malloc command_string: %r");
 }
 if((ls_out=malloc(STR_CHARS)) == 0){
    sysfatal("can't malloc ls_out: %r");
 }
 if((conffile=malloc(STR_CHARS)) == 0){
    sysfatal("can't malloc conffile: %r");
 }

 if((tbuf=malloc(10 * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
 }
	
 poptcon = poptGetContext(NULL, largc, (const char **) largv, poptopt, 0);
 
 if((rc = poptGetNextOpt(poptcon)) != -1){
     fprintf(stderr,"%s: Invalid flag supplied.\n",progname);
     exit(EXIT_FAILURE);
 }
 nport=poptGetArg(poptcon);
 
 if(version) {
     printf("%s Version: %s\n",progname,VERSION);
     exit(EXIT_SUCCESS);
 }   
 if(port) {
     if ( port < 1 || port > 65535) {
         fprintf(stderr,"%s: Invalid port supplied.\n",progname);
         exit(EXIT_FAILURE);
     }
 }else if(nport){
     port=nport;
     if ( port < 1 || port > 65535) {
         fprintf(stderr,"%s: Invalid port supplied.\n",progname);
         exit(EXIT_FAILURE);
     }
 }else{
  port=DEFAULT_PORT;
 }   

 if(debug <=0){
    debug=0;
 }
  
 if(!binpath && (ebinpath=getenv("LSF_BIN_PATH"))!=NULL){
     binpath=ebinpath;
 }
 
 if(!confpath && (econfpath=getenv("LSF_CONF_PATH"))!=NULL){
     confpath=econfpath;
 } 
 
 sprintf(conffile,"%s/lsf.conf",confpath);
 
 if((fp=fopen(conffile, "r")) != 0){
  while(fgets(line, STR_CHARS, fp)){
   if(strstr(line,"LSB_SHAREDIR")!=0){
    break;
   }
  }
 } else {
  fprintf(stderr,"%s: Cannot open %s file.\n",progname,conffile);
  exit(EXIT_FAILURE);
 }
 fclose(fp);

 maxtok=strtoken(line,'=',tbuf);
 if(tbuf[1]){
  lsf_base_pathtmp=strdup(tbuf[1]);
 } else {
  fprintf(stderr,"%s: Unable to find logdir in conf file.\n",progname);
  exit(EXIT_FAILURE);  
 }
 
 if ((cp = strrchr (lsf_base_pathtmp, '\n')) != NULL){
  *cp = '\0';
 }
 
 lsf_base_path=strdel(lsf_base_pathtmp, "\" ");
 free(lsf_base_pathtmp);
 
 s=(char*)malloc(strlen(binpath)+strlen("lsid")+2);
 sprintf(s,"%s/lsid",binpath);
 rc=stat(s,&sbuf);
 if(rc) {
   fprintf(stderr,"%s not found\n",s);
   exit(EXIT_FAILURE);
 }
 if( ! (sbuf.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ) {
   /* lsid is not executable for anybody, and is thus useless */
   fprintf(stderr,"%s: %s is not executable, but mode %05o\n",progname,s,(int)sbuf.st_mode);
   exit(EXIT_FAILURE);
 }
 free(s);

 sprintf(command_string,"%s/lsid | grep 'My cluster name is'|awk -F\" \" '{ print $5 }'",binpath);
 file_output = popen(command_string,"r");
 
 if (file_output != NULL){
  len = fread(lsf_clustername, sizeof(char), STR_CHARS - 1 , file_output);
  if (len>0){
   lsf_clustername[len-1]='\000';
  }
 }
 pclose(file_output);
 
 sprintf(logpath,"%s/%s/logdir",lsf_base_path,lsf_clustername);
 
 for(ii=0;ii<maxtok;ii++){
  free(tbuf[ii]);
 }
 free(line);
 free(tbuf);
 free(lsf_base_path);
 free(lsf_clustername);
 free(ls_out);
 free(conffile);
 free(command_string);
 
 return logpath;

}

char *GetLogList(char *logdate){
         DIR             *dirh;
         struct dirent   *direntry;
         int             rc;
         struct stat     sbuf;
         time_t          tage;
         char            *s,*p,*dir;
         struct tm       tmthr;
         char            *slogs;

         if((slogs=calloc(MAX_CHARS,1)) == 0){
                 sysfatal("can't malloc slogs: %r");
         }
	 
         tmthr.tm_sec=tmthr.tm_min=tmthr.tm_hour=tmthr.tm_isdst=0;
         p=strptime(logdate,"%Y%m%d",&tmthr);
         if( (p-logdate) != 8) {
                 fprintf(stderr,"%s: Timestring \"%s\" is invalid (YYYYmmdd)\n",progname,logdate);
                 return NULL;
         }
         tage=mktime(&tmthr);

         if( !(dirh=opendir(ldir)) ) {
                 syserror("Cannot open directory %s: %r", ldir);
                 return NULL;
         }

         while ( (direntry=readdir(dirh)) ) {
                 if( *(direntry->d_name) == '.' ) continue;
                 if(!(s=(char*)malloc(strlen(direntry->d_name)+strlen(ldir)+2))) {
                         fprintf(stderr,"%s: Cannot alloc string space\n",progname);
                         return NULL;
                 }
                 sprintf(s,"%s/%s",ldir,direntry->d_name);
                 rc=stat(s,&sbuf);
                 if(rc) {
                         syserror("Cannot stat file %s: %r", s);
                         return NULL;
                 }
                 if ( sbuf.st_mtime > tage ) {
			 if(strstr(s,lsbevents)!=NULL && strstr(s,"lock")==NULL && strstr(s,"index")==NULL){
		            strcat(slogs,s);
                            strcat(slogs," ");
			 }
                 }  

                 free(s);
         }

         closedir(dirh);
	 
	 return(slogs);
}

void CreamConnection(int c_sock){ 

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

   if((buffer=malloc(STR_CHARS)) == 0){
    sysfatal("can't malloc buffer in CreamConnection: %r");
   }

   while ( 1 ) {
	  
    retcod = poll(pfds, nfds, timeout); 
        
    if(retcod <0){
     fprintf(stderr, "%s: Poll error for Cream\n",progname);
     close(conn_c);
     exit(EXIT_FAILURE);
    }
    
    if ( retcod > 0 ){
         if ( ( fds[0].revents & ( POLLERR | POLLNVAL | POLLHUP) )){
           switch (fds[0].revents){
           case POLLNVAL:
		   fprintf(stderr, "%s: poll() file descriptor error for Cream\n",progname);
        	   break;
           case POLLHUP:
		   fprintf(stderr, "%s: Connection closed for Cream\n",progname);
        	   break;
           case POLLERR:
		   fprintf(stderr, "%s: poll() POLLERR for Cream\n",progname);
        	   break;
           }
        } else {
            
	  if ( (conn_c = accept(c_sock, NULL, NULL) ) < 0 ) {
	   fprintf(stderr, "%s: Error calling accept()\n",progname);
	   exit(EXIT_FAILURE);
          }
	    
     	  buffer[0]='\0';
     	  Readline(conn_c, buffer, STR_CHARS-1);
	  if(debug){
	   fprintf(debuglogfile, "Received for Cream:%s",buffer);
           fflush(debuglogfile);
	  }
	  if(buffer && (strstr(buffer,"STARTNOTIFY")!=NULL)){
	   NotifyFromDate(buffer);
	  }
	
	} 
     }       
   }       
    
}

int NotifyFromDate(char *in_buf){

    char * out_buf;
    int    ii;
    char *notstr;
    char *notdate;
    char *lnotdate;
    int   notepoch;
    int   logepoch;

    int  maxtok,j; 
    char **tbuf;
    char *cp;

    /* printf("thread/0x%08lx\n",pthread_self()); */

    if((out_buf=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc out_buf: %r");
    }
    
    if((tbuf=malloc(10 * sizeof *tbuf)) == 0){
      sysfatal("can't malloc tbuf: %r");
    }
       
    maxtok=strtoken(in_buf,'/',tbuf);
    
    if(tbuf[0]){
     notstr=strdup(tbuf[0]);
    }
    if(tbuf[1]){
     notdate=strdup(tbuf[1]);
     if ((cp = strrchr (notdate, '\n')) != NULL){
      *cp = '\0';
     }
    }
    
    for(j=0;j<maxtok;j++){
     free(tbuf[j]);
    }
    free(tbuf);
            
    if(notstr && strcmp(notstr,"STARTNOTIFY")==0){
    
      creamisconn=1;
      
      notepoch=str2epoch(notdate,"S");
      
      if(cream_recycled){
       logepoch=nti[jcount+1];
      }else{
       logepoch=nti[0];
      }      

      if(notepoch<=logepoch){
       lnotdate=iepoch2str(notepoch);
       GetEventsInOldLogs(lnotdate);
      }
      
      if(cream_recycled){

       for(ii=jcount+1;ii<CRMHASHSIZE;ii++){
        if(notepoch<=nti[ii]){
         sprintf(out_buf,"NTFDATE/%s",ntf[ii]);
         Writeline(conn_c, out_buf, strlen(out_buf));
         if(debug){
          fprintf(debuglogfile, "Sent for Cream_nftdate:%s",out_buf);
          fflush(debuglogfile); 
         }
        }
       }

      }
            
      for(ii=0;ii<=jcount;ii++){
       if(notepoch<=nti[ii]){
        sprintf(out_buf,"NTFDATE/%s",ntf[ii]);  
        Writeline(conn_c, out_buf, strlen(out_buf));
	if(debug){
	 fprintf(debuglogfile, "Sent for Cream_nftdate:%s",out_buf);
         fflush(debuglogfile);
	}
       }
      }
      Writeline(conn_c, "NTFDATE/END\n", strlen("NTFDATE/END\n"));
      
      free(out_buf);
      free(notstr);
      free(notdate);
      
      return 0;    
    }
    
    free(out_buf);
    free(notstr);
    free(notdate);
    	    
    return -1;
}

int NotifyCream(int jobid, char *newstatus, char *blahjobid, char *wn, char *reason, char *timestamp, int flag){

 /*if flag ==0 AddToStruct is called within GetOldLogs 
   if flag ==1 AddToStruct is called elsewhere*/
   
    char     *buffer;
    char     *outreason;
    char     *sjobid;
  
    int      retcod;
        
    struct   pollfd fds[2];   /*      poll file descp. struct	       */
    struct   pollfd *pfds;    /*      pointer to fds		       */
    int      nfds = 1;
    int      timeout= 1;
    
    char    **clientjobid;
    int      maxtok,i;
    
    fds[0].fd = conn_c;
    fds[0].events = 0;
    fds[0].events = ( POLLIN | POLLOUT | POLLPRI | POLLERR | POLLHUP | POLLNVAL ) ;
    pfds = fds;
    
    
    if((buffer=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc buffer: %r");
    }
    if((outreason=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc outreason: %r");
    }
    if((clientjobid=malloc(10 * sizeof *clientjobid)) == 0){
       sysfatal("can't malloc clientjobid %r");
    }
    if((sjobid=malloc(10 * sizeof *sjobid)) == 0){
       sysfatal("can't malloc sjobid %r");
    }
    
    sprintf(sjobid, "%d",jobid);
    
    buffer[0]='\0';
    outreason[0]='\0';

    if(strcmp(reason,"NA")!=0){
      sprintf(outreason," Reason=\"lsf_reason=%s\";" ,reason);
    }
    
    maxtok = strtoken(blahjobid, '_', clientjobid);    
    
    if(strcmp(wn,"NA")!=0){
      sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=%s; ClientJobId=\"%s; WorkerNode=%s;%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], wn, outreason, timestamp);
    }else{
      sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=%s; ClientJobId=\"%s;%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], outreason, timestamp);
    }
    
    for(i=0;i<maxtok;i++){
     free(clientjobid[i]);
    }
    free(clientjobid);

    free(sjobid);
    
    /* set lock for cream cache */
    pthread_mutex_lock( &cr_write_mutex );

    if(jcount>=CRMHASHSIZE){
     jcount=1;
     cream_recycled=1;
     if(debug>=3){
      fprintf(debuglogfile, "Cream Counter Recycled\n");
      fflush(debuglogfile);
     }  
    } 
    
    nti[jcount]=str2epoch(timestamp,"S");
    ntf[jcount++]=strdup(buffer);
    
    /* unset lock for cream cache */
    pthread_mutex_unlock( &cr_write_mutex );
    
    if((creamisconn==0) || (flag==0)){
     free(buffer);
     free(outreason);
     return -1;
    }
    
    
    retcod = poll(pfds, nfds, timeout); 
        
    if(retcod <0){
     fprintf(stderr, "%s: Poll error for Cream\n",progname);
     close(conn_c);
     exit(EXIT_FAILURE);
    }
    
    if ( retcod > 0 ){
        if ( ( fds[0].revents & ( POLLERR | POLLNVAL | POLLHUP) )){
           switch (fds[0].revents){
           case POLLNVAL:
		   fprintf(stderr, "%s: poll() file descriptor error for Cream\n",progname);
        	   break;
           case POLLHUP:
		   fprintf(stderr, "%s: Connection closed for Cream\n",progname);
        	   break;
           case POLLERR:
		   fprintf(stderr, "%s: poll() POLLERR for Cream\n",progname);
        	   break;
           }
        } else {
	  Writeline(conn_c, buffer, strlen(buffer));
	  if(debug){
	   fprintf(debuglogfile, "Sent for Cream:%s",buffer);
           fflush(debuglogfile);
	  }
	} 
     }       
			
    free(buffer);
    free(outreason);
    
    return 0;
    
}

int UpdatePtr(int jid){

 int rid;
 
 if((jid <= 0)){
  return -1;
 }
 
 /* if it is over RDXHASHSIZE the ptrcnt is recycled */
 if(ptrcnt>=RDXHASHSIZE){
  ptrcnt=1;
  recycled=1;  
  if(debug>=3){
    fprintf(debuglogfile, "Counter Recycled\n");
    fflush(debuglogfile);
  }  
 }
 
 
 if((rid=GetRdxId(jid))==-1){
  if(debug>=3){
    fprintf(debuglogfile, "JobidNew Counter:%d jobid:%d\n",ptrcnt,jid);
    fflush(debuglogfile);
  }
  rptr[ptrcnt++]=jid;
  return(ptrcnt-1);
 }else{
  if(debug>=3){
    fprintf(debuglogfile, "JobidOld Counter:%d jobid:%d\n",rid,jid);
    fflush(debuglogfile);
  }
  return rid;
 }
  
}

int GetRdxId(int cnt){
  int i;
  for(i=0;i<RDXHASHSIZE;i++){
   if(rptr[i] == cnt){
    return i;
   }
  }
 return -1;
}

int strtoken(const char *s, char delim, char **token){
    char *tmp;
    char *ptr, *dptr;
    int i = 0;
    
    tmp = (char *) malloc(1 + strlen(s));
    assert(tmp);
    strcpy(tmp, s);
    ptr = tmp;
    while(1) {
        if((dptr = strchr(ptr, delim)) != NULL) {
            *dptr = '\0';
	    token[i] = (char *) malloc(1 + strlen(ptr));
	    assert(token[i]);
            strcpy(token[i], ptr);
            ptr = dptr + 1;
	    if (strlen(token[i]) != 0){
             i++;
	    }
        } else {
            if(strlen(ptr)) {
		token[i] = (char *) malloc(1 + strlen(ptr));
		assert(token[i]);
                strcpy(token[i], ptr);
                i++;
                break;
            } else{
	        break;
	    }
        }
    }
    
    token[i] = NULL;
    free(tmp);
    return i;
}

char *strdel(char *s, const char *delete){
    char *tmp, *cptr, *sptr;
    
    if(!delete || !strlen(delete))
        return s;
        
    if(!s || !strlen(s))
        return s;
        
    tmp = strndup(s, STR_CHARS);
       
    assert(tmp);
    
    for(sptr = tmp; (cptr = strpbrk(sptr, delete)); sptr = tmp) {
        *cptr = '\0';
        strcat(tmp, ++cptr);
    }
    
    return tmp;
}

char *epoch2str(char *epoch){
  
 char *dateout;
 size_t max=100;

 struct tm *tm;
 if((tm=malloc(max)) == 0){
  sysfatal("can't malloc tm in epoch2str: %r");
 }

 strptime(epoch,"%s",tm);
 
 dateout=malloc(max);
 
 strftime(dateout,max,"%Y-%m-%d %T",tm);
 free(tm);
 
 return dateout;
 
}

char *iepoch2str(int epoch){
  
 char *dateout;
 char *lepoch;
 size_t max=100;

 struct tm *tm;
 if((tm=malloc(max)) == 0){
  sysfatal("can't malloc tm in iepoch2str: %r");
 }
 if((lepoch=malloc(STR_CHARS)) == 0){
  sysfatal("can't malloc lepoch in iepoch2str: %r");
 }
 
 sprintf(lepoch,"%d",epoch);
 
 strptime(lepoch,"%s",tm);
 
 dateout=malloc(max);
 
 strftime(dateout,max,"%Y%m%d%H%M.%S",tm);
 free(tm);
 free(lepoch);
 
 return dateout;
 
}

int str2epoch(char *str, char * f){
  
 char *dateout;
 int idate;
 size_t max=100;

 struct tm *tm;
 if((tm=malloc(max)) == 0){
  sysfatal("can't malloc tm in str2epoch: %r");
 }
 if(strcmp(f,"S")==0){
  strptime(str,"%Y-%m-%d %T",tm);
 }else if(strcmp(f,"L")==0){
  strptime(str,"%a %b %d %T %Y",tm);
 }
 
 dateout=malloc(max);
 
 strftime(dateout,max,"%s",tm);
 
 free(tm);
 
 idate=atoi(dateout);
 free(dateout);
 
 return idate;
 
}

void daemonize(){

    int pid;
    
    pid = fork();
    if (pid < 0)
    {
        fprintf(stderr,"%s: Cannot fork.\n",progname);
        exit(EXIT_FAILURE);
    }
    else if (pid >0)
    {
        exit(EXIT_SUCCESS);
    }
    
    setsid();
    
    pid = fork();
    if (pid < 0)
    {
        fprintf(stderr,"%s: Cannot fork.\n",progname);
        exit(EXIT_FAILURE);
    }
    else if (pid >0)
    {
        exit(EXIT_SUCCESS);
    }
    chdir("/");
    umask(0);
    
  freopen ("/dev/null", "r", stdin);  
  freopen ("/dev/null", "w", stdout);
  freopen ("/dev/null", "w", stderr); 

}

void eprint(int err, char *fmt, va_list args){
    extern int errno;

    fprintf(stderr, "%s: ", argv0);
    if(fmt)
        vfprintf(stderr, fmt, args);
    if(err)
        fprintf(stderr, "%s", strerror(errno));
    fputs("\n", stderr);
    errno = 0;
}

char *chopfmt(char *fmt){
    static char errstr[ERRMAX];
    char *p;

    errstr[0] = '\0';
    if((p=strstr(fmt, "%r")) != 0)
        fmt = strncat(errstr, fmt, p-fmt);
    return fmt;
}

/* syserror: print error and continue */
void syserror(char *fmt, ...){
    va_list args;
    char *xfmt;

    va_start(args, fmt);
    xfmt = chopfmt(fmt);
    eprint(xfmt!=fmt, xfmt, args);
    va_end(args);
}

/* sysfatal: print error and die */
void sysfatal(char *fmt, ...){
    va_list args;
    char *xfmt;

    va_start(args, fmt);
    xfmt = chopfmt(fmt);
    eprint(xfmt!=fmt, xfmt, args);
    va_end(args);
    exit(EXIT_FAILURE);
}
