#include "BLParserPBS.h"

int main(int argc, char *argv[]) {

    struct    sockaddr_in servaddr;
    char      *endptr;
    int       i,j;
    int       set = 1;
    int       status;
    int       list_s;
    int       list_c;
    char     *Cendptr;
    
    char      *eventsfile;
    
    time_t now;
    struct tm *tptr;
   
    char *szPort;
    char *szSpoolDir;
    char *szDebugLogName;
    char *szDebugLevel;
    
    char *espooldir;
    FILE      *fpt;

    pthread_t ReadThd[NUMTHRDS];
    pthread_t UpdateThd;
    pthread_t CreamThd;

    argv0 = argv[0];

    /*Ignore sigpipe*/
    
    signal(SIGPIPE, SIG_IGN);             
        
    ParseCmdLine(argc, argv, &szPort, &szSpoolDir, &szCreamPort, &szDebugLogName, &szDebugLevel);
    
    if(dmn){    
     daemonize();
    }
    if((argc > 1) && (szDebugLevel!=NULL)){
     debug = strtol(szDebugLevel, &endptr, 0);
     if (debug <=0){
      debug=0;
     }
    }
    
    if((argc > 1) && (szPort!=NULL)){
     port = strtol(szPort, &endptr, 0);
     if ( *endptr || port < 1 || port > 65535) {
       fprintf(stderr,"%s: Invalid port supplied.\n",progname);
       exit(EXIT_FAILURE);
     }
    }else{
     port=DEFAULT_PORT;
    }
        
    /* Get log dir name */
  
    if((ldir=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc line: %r");
    }
    
    if(szDebugLogName!=NULL){
     debuglogname=szDebugLogName;
    }
    if(debug){
     if((debuglogfile = fopen(debuglogname, "a+"))==0){
      debuglogfile =  fopen("/dev/null", "a+");
     }
    }
    
    if(szSpoolDir!=NULL){
     spooldir=szSpoolDir;
    }else if((espooldir=getenv("PBS_SPOOL_DIR"))!=NULL){
     spooldir=espooldir;
    }
        
    strcat(ldir,spooldir);
    strcat(ldir,"/server_logs");
    
    now=time(NULL);
    tptr=localtime(&now);
    strftime(cnow,sizeof(cnow),"%Y%m%d",tptr);

    if((eventsfile=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc eventsfile: %r");
    }
    
    strcat(eventsfile,ldir);
    strcat(eventsfile,"/");
    strcat(eventsfile,cnow);

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
	fprintf(stderr, "%s: Error calling bind()\n",progname);
	exit(EXIT_FAILURE);
    }
    
    if ( listen(list_s, LISTENQ) < 0 ) {
    	fprintf(stderr, "%s: Error calling listen()\n",progname);
    	exit(EXIT_FAILURE);
    }
    
    /* create listening socket for Cream */

    if(usecream>0){
      creamport = strtol(szCreamPort, &Cendptr, 0);
      
      if ( *Cendptr ) {
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

    char   *tdir;
    time_t lnow;
    struct tm *timeptr;
    char   tnow[30];
    char   evfile[STR_CHARS]="\0";
    char   *actualfile;
   
    if((tdir=calloc(STR_CHARS,1)) == 0){
     sysfatal("can't malloc tdir: %r");
    }
    strcat(tdir,spooldir);
    strcat(tdir,"/server_logs");

    actualfile=strdup(infile);
	
    for(;;){

/* In each cycle a new date file is costructed and is tested with the existing one
   when the date changes the new log file can be created later so we test if it is there
*/
     
        lnow=time(NULL);
        timeptr=localtime(&lnow);
        strftime(tnow,sizeof(tnow),"%Y%m%d",timeptr);

        evfile[0]='\0';
        strcat(evfile,tdir);
        strcat(evfile,"/");
        strcat(evfile,tnow);
	
        if(strcmp(evfile,actualfile) != 0){

         off = 0;
	 actualfile=strdup(evfile);

         while(1){
          if((fp=fopen((char *)evfile, "r")) != 0){
           break;
          }
          sleep (1);
         }

        }else{

         if((fp=fopen((char *)actualfile, "r")) == 0){
          syserror("error opening %s: %r", actualfile);
          continue;
	  sleep(1);
         }

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
         off=real_off;
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
      if((strstr(line,rex_queued)!=NULL) || (strstr(line,rex_running)!=NULL) || (strstr(line,rex_deleted)!=NULL) || (strstr(line,rex_finished)!=NULL) || (strstr(line,rex_hold)!=NULL)){
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

 char *	jobid=NULL;
 unsigned h_blahjob;
 
  
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
  j2ec[id] = strdup("\0");
  j2st[id] = strdup("\0");
  j2rt[id] = strdup("\0");
  j2ct[id] = strdup("\0");
  
 } else if((strcmp(flag,"JOBID")==0) && recycled==1){
 
  free(j2js[id]);
  free(j2bl[id]);
  free(j2ec[id]);
  free(j2st[id]);
  free(j2rt[id]);
  free(j2ct[id]);
  
  j2js[id] = strdup("1");  
  j2bl[id] = strdup("\0");
  j2ec[id] = strdup("\0");
  j2st[id] = strdup("\0");
  j2rt[id] = strdup("\0");
  j2ct[id] = strdup("\0");
    
 } else if(strcmp(flag,"BLAHPNAME")==0){
 
  if((jobid=malloc(STR_CHARS)) == 0){
     sysfatal("can't malloc jobid: %r");
  }
  free(j2bl[id]);
  j2bl[id] = strdup(value);
  
  h_blahjob=hash(value);
  sprintf(jobid,"%d",id);
  bjl[h_blahjob]=strdup(jobid);
  free(jobid);
  
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

 int has_blah=0;
 
 char *	trex;
 char *	rex;

 int  maxtok,ii; 
 char **tbuf;
 
 int id;
 int is_queued=0;
 int is_finished=0;
 
 char *	tjobid=NULL;
 char *	jobid=NULL;

 char *	tj_time=NULL;
 char *	j_time=NULL;

 char *	tex_status=NULL;
 char *	ex_status=NULL;

 char *tb_job=NULL;
 char *tj_blahjob=NULL;
 char *j_blahjob=NULL;
 
 if((strstr(line,blahjob_string)!=NULL) || (strstr(line,cream_string)!=NULL)){
  has_blah=1;
 }
 
 if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
 }

 maxtok=strtoken(line,';',tbuf);
  
 if(maxtok>0){
  tj_time=strdup(tbuf[0]);
  j_time=convdate(tj_time);
 }
 if(maxtok>4){
  tjobid=strdup(tbuf[4]);
 }
 if(maxtok>5){
  rex=strdup(tbuf[5]);
  trex=strdup(rex);
 }

 for(ii=0;ii<maxtok;ii++){
  free(tbuf[ii]);
 }
 free(tbuf);

/* get jobid */ 

 if(tjobid){

  if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
  }
  
  maxtok=strtoken(tjobid,'.',tbuf);
 
  if(maxtok>0){
   jobid=strdup(tbuf[0]);
  }

  for(ii=0;ii<maxtok;ii++){
   free(tbuf[ii]);
  }
  free(tbuf);
 

 } /* close tjobid if */

  id=UpdatePtr(atoi(jobid));
  
/* get j_blahjob */

 if(rex && (strstr(rex,rex_queued)!=NULL)){
  is_queued=1; 

  if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
  }
  maxtok=strtoken(trex,',',tbuf);
  
  if(maxtok>2){
   tb_job=strdup(tbuf[2]);
  }
  
  for(ii=0;ii<maxtok;ii++){
   free(tbuf[ii]);
  }
  free(tbuf);

  if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
  }
  maxtok=strtoken(tb_job,'=',tbuf);
  
  if(maxtok>1){
   tj_blahjob=strdup(tbuf[1]);
  }
  
  for(ii=0;ii<maxtok;ii++){
   free(tbuf[ii]);
  }
  free(tbuf);

  if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
  }
  maxtok=strtoken(tj_blahjob,' ',tbuf);

  if(maxtok>0){
   j_blahjob=strdup(tbuf[0]);
  }else{
   j_blahjob=strdup(tj_blahjob);
  }

  for(ii=0;ii<maxtok;ii++){
   free(tbuf[ii]);
  }
  free(tbuf);


   
 } /* close rex_queued if */

/* get ex_status */
 if(rex && (strstr(rex,rex_finished)!=NULL)){
  is_finished=1;
  
  if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
  }
  maxtok=strtoken(trex,' ',tbuf);
  
  if(maxtok>0){
   tex_status=strdup(tbuf[0]);
  }
  
  for(ii=0;ii<maxtok;ii++){
   free(tbuf[ii]);
  }
  free(tbuf);
  
  if((tbuf=malloc(TBUFSIZE * sizeof *tbuf)) == 0){
     sysfatal("can't malloc tbuf: %r");
  }
  maxtok=strtoken(tex_status,'=',tbuf);
  
  if(maxtok>1){
   ex_status=strdup(tbuf[1]);
  }
  
  for(ii=0;ii<maxtok;ii++){
   free(tbuf[ii]);
  }
  free(tbuf);
  
 } /* close rex_finished if */
 
 while (1){
  if(rcounter==0){
   break;
  }
  sleep(1);
 } 
 
 if((is_queued==1) && (has_blah)){

  InfoAdd(id,jobid,"JOBID");
  InfoAdd(id,j_time,"STARTTIME");
  InfoAdd(id,j_blahjob,"BLAHPNAME");

  if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
   NotifyCream(id, "1", j2bl[id], "NA", "NA", j2st[id], flag);
  }
  
 } else if(j2bl[id] && ((strstr(j2bl[id],blahjob_string)!=NULL) || (strstr(j2bl[id],cream_string)!=NULL))){ 
 
  if(rex && strstr(rex,rex_running)!=NULL){

   InfoAdd(id,"2","JOBSTATUS");
   InfoAdd(id,j_time,"RUNNINGTIME");
   
   if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
    NotifyCream(id, "2", j2bl[id], "NA", "NA", j2rt[id], flag);
   }
   
  } else if(strstr(rex,rex_deleted)!=NULL){
  
   InfoAdd(id,"3","JOBSTATUS");

   if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
    NotifyCream(id, "3", j2bl[id], "NA", "NA", j_time, flag);
   }
    
  } else if(is_finished==1){
  
   InfoAdd(id,"4","JOBSTATUS");
   InfoAdd(id,ex_status,"EXITCODE");
   InfoAdd(id,j_time,"COMPLTIME");

   if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
    NotifyCream(id, "4", j2bl[id], "NA", ex_status, j2ct[id], flag);
   }

  } else if(rex && ((strstr(rex,rex_uhold)!=NULL) || (strstr(rex,rex_ohold)!=NULL) || (strstr(rex,rex_ohold)!=NULL))){
   
   if(strcmp(j2js[id],"1")==0){
    InfoAdd(id,"5/1","JOBSTATUS");
   }else if(strcmp(j2js[id],"2")==0){
    InfoAdd(id,"5/2","JOBSTATUS");
   }
   
   if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
    NotifyCream(id, "5", j2bl[id], "NA", "NA", j_time, flag);
   }
   
  } else if(rex && ((strstr(rex,rex_uresume)!=NULL) || (strstr(rex,rex_oresume)!=NULL) || (strstr(rex,rex_oresume)!=NULL))){
   
   if(strcmp(j2js[id],"5/1")==0){
    InfoAdd(id,"1","JOBSTATUS");
    if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
     NotifyCream(id, "1", j2bl[id], "NA", "NA", j_time, flag);
    }
   }else if(strcmp(j2js[id],"5/2")==0){
    InfoAdd(id,"2","JOBSTATUS");
    if((usecream>0) && j2bl[id] && (strstr(j2bl[id],cream_string)!=NULL)){
     NotifyCream(id, "2", j2bl[id], "NA", "NA", j_time, flag);
    }
   }
   
  } /* closes if-else if on rex_ */
 } /* closes if-else if on jobid lookup */
 
   free(rex);
   free(tjobid);
   free(jobid);
   free(tex_status);
   free(ex_status);
   free(tb_job);
   free(tj_blahjob);
   free(j_blahjob);

 return 0;
}

char *GetAllEvents(char *file){
 
 FILE *fp;
 char *line;
 char **opfile;
 int i=0;
 int maxtok;

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
    if((strstr(line,rex_queued)!=NULL) || (strstr(line,rex_running)!=NULL) || (strstr(line,rex_deleted)!=NULL) || (strstr(line,rex_finished)!=NULL) || (strstr(line,rex_hold)!=NULL)){
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
    char      *jstat;
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
	if((jstat=malloc(STR_CHARS)) == 0){
	  sysfatal("can't malloc jstat in LookupAndSend: %r");
	}
        jstat[0]='\0';
	
	Readline(conn_s, buffer, STR_CHARS-1);
	if(debug){
	 fprintf(debuglogfile, "Received:%s",buffer);
         fflush(debuglogfile);
	}
	
	/* printf("thread/0x%08lx\n",pthread_self()); */
	
	if((strlen(buffer)==0) || (strcmp(buffer,"\n")==0) || (strstr(buffer,"/")==0)){
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
     	 sprintf(out_buf,"Wrong string format/Not\n");
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
	
	if((strlen(logdate)==0) || (strcmp(logdate,"\n")==0) || ((strcmp(logdate,"CREAMPORT")!=0) && ((strlen(jobid)==0) || (strcmp(jobid,"\n")==0)))){
         if((out_buf=malloc(STR_CHARS)) == 0){
          sysfatal("can't malloc out_buf in LookupAndSend: %r");
         }
         sprintf(out_buf,"\n");

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
	
/* get jobid from blahjob id (needed by *_submit.sh) */
       
	if(strcmp(logdate,"BLAHJOB")==0){
         for(i=0;i<WRETRIES;i++){
	  if(wlock==0){
	   if(bjl[hash(jobid)]==NULL){
	    sleep(1);
	    continue;
	   }
           if((out_buf=malloc(STR_CHARS)) == 0){
            sysfatal("can't malloc out_buf in LookupAndSend: %r");
           }
     	   sprintf(out_buf,"%s\n",bjl[hash(jobid)]);
	   goto close;
	  }else{
	   sleep(1);
	  } 
	 }
	 if(i==WRETRIES){
          if((out_buf=malloc(STR_CHARS)) == 0){
           sysfatal("can't malloc out_buf in LookupAndSend: %r");
          }
	  sprintf(out_buf,"Blahjob id %s not found\n",jobid);
	  goto close;
	 }
	}
	
/* get all info from jobid */

        for(i=0;i<WRETRIES;i++){
	
	 if(wlock==0){
	 
          id=GetRdxId(atoi(jobid));
	  
    	  if(id>0 && j2js[id]!=NULL){
 
           if((out_buf=malloc(STR_CHARS)) == 0){
            sysfatal("can't malloc out_buf in LookupAndSend: %r");
           }
	   	   
           if((strcmp(j2js[id],"3")==0) || (strcmp(j2js[id],"4")==0)){
            pr_removal="Yes";
           } else {
            pr_removal="Not";
           }
           if((strcmp(j2js[id],"5/1")==0) || (strcmp(j2js[id],"5/2")==0)){
            sprintf(jstat," JobStatus=5;");
	   }else{
            sprintf(jstat," JobStatus=%s;",j2js[id]);
	   }
	   
           if(strcmp(j2js[id],"4")==0){
            sprintf(out_buf,"[BatchJobId=\"%s\";%s LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\"; LRMSCompletedTime=\"%s\"; ExitCode=%s;/%s\n",jobid, jstat, j2st[id], j2rt[id], j2ct[id], j2ec[id], pr_removal);
           }else if(strcmp(j2rt[id],"\0")!=0){
            sprintf(out_buf,"[BatchJobId=\"%s\";%s LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\";/%s\n",jobid, jstat, j2st[id], j2rt[id], pr_removal);
           }else{
            sprintf(out_buf,"[BatchJobId=\"%s\";%s LRMSSubmissionTime=\"%s\";/%s\n",jobid, jstat, j2st[id], pr_removal);
           }
	   
	  } else {
	  
     	   GetEventsInOldLogs(logdate);
	   
           id=GetRdxId(atoi(jobid));
	   
     	   if(id>0 && j2js[id]!=NULL){

            if((out_buf=malloc(STR_CHARS)) == 0){
             sysfatal("can't malloc out_buf in LookupAndSend: %r");
            }
	    
            if((strcmp(j2js[id],"3")==0) || (strcmp(j2js[id],"4")==0)){
             pr_removal="Yes";
            } else {
             pr_removal="Not";
            }
            if((strcmp(j2js[id],"5/1")==0) || (strcmp(j2js[id],"5/2")==0)){
             sprintf(jstat," JobStatus=5;");
	    }else{
             sprintf(jstat," JobStatus=%s;",j2js[id]);
	    }
	    
            if(strcmp(j2js[id],"4")==0){
             sprintf(out_buf,"[BatchJobId=\"%s\";%s LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\"; LRMSCompletedTime=\"%s\"; ExitCode=%s;/%s\n",jobid, jstat, j2st[id], j2rt[id], j2ct[id], j2ec[id], pr_removal);
            }else if(strcmp(j2rt[id],"\0")!=0){
             sprintf(out_buf,"[BatchJobId=\"%s\";%s LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\";/%s\n",jobid, jstat, j2st[id], j2rt[id], pr_removal);
            }else{
             sprintf(out_buf,"[BatchJobId=\"%s\";%s LRMSSubmissionTime=\"%s\";/%s\n",jobid, jstat, j2st[id], pr_removal);
            }
	    
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
        free(jstat);
	
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

char *GetLogList(char *logdate){
 
 char *datefile;
 char *touch_out;
 char *rm_out;
 char *logs;
 char *slogs;
 char *tlogs;
 char *command_string;
 FILE *mktemp_output;
 FILE *touch_output;
 FILE *find_output;
 FILE *rm_output;
 FILE *ls_output;
 int len;
 int last_tag;
 int maxtok;
 int i=0;
 char **oplogs;

 if((logs=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc logs: %r");
 }
 if((slogs=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc slogs: %r");
 }
 if((tlogs=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc tlogs: %r");
 }
 if((command_string=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc command_string: %r");
 }
 if((datefile=malloc(STR_CHARS)) == 0){
  sysfatal("can't malloc datefile: %r");
 }
 if((touch_out=malloc(STR_CHARS)) == 0){
  sysfatal("can't malloc touch_out: %r");
 }
 if((rm_out=malloc(STR_CHARS)) == 0){
  sysfatal("can't malloc rm_out: %r");
 }
 
 sprintf(command_string,"mktemp -q /tmp/blahdate_XXXXXX");
 mktemp_output = popen(command_string,"r");
 if (mktemp_output != NULL){
  len = fread(datefile, sizeof(char), STR_CHARS - 1 , mktemp_output);
  if (len>0){
   datefile[len-1]='\000';
  }
 }
 pclose(mktemp_output);

/* We deal with both date format (20050513 and 200505130000.00) even if it is not needed */

 if(strlen(logdate) > 9){
  sprintf(command_string,"touch -t %s %s 2>/dev/null",logdate,datefile);
 } else {
  sprintf(command_string,"touch -d %s %s 2>/dev/null",logdate,datefile);
 }

 touch_output = popen(command_string,"r");
 if (touch_output != NULL){
  len = fread(touch_out, sizeof(char), STR_CHARS - 1 , touch_output);
  if (len>0){
   touch_out[len-1]='\000';
  }
 }
 pclose(touch_output);
 
 sprintf(command_string,"find %s/* -type f -newer %s -printf \"%%p \" 2>/dev/null", ldir, datefile);
 find_output = popen(command_string,"r");
 if (find_output != NULL){
  len = fread(logs, sizeof(char), MAX_CHARS - 1 , find_output);
  if (len>0){
   logs[len-1]='\000';
  }
 }
 pclose(find_output);
  
 sprintf(command_string,"rm %s", datefile);
 rm_output = popen(command_string,"r");
 if (rm_output != NULL){
  len = fread(rm_out, sizeof(char), STR_CHARS - 1 , rm_output);
  if (len>0){
   rm_out[len-1]='\000';
  }
 }
 pclose(rm_output);
 
/* this is done to avoid ls -tr to run without args so that local dir is listed */

 if((logs == NULL) || (strlen(logs) < 2)){
  free(command_string);
  free(datefile);
  free(touch_out);
  free(rm_out);
  free(logs);
  free(tlogs);
  free(slogs);
  return NULL;
 }
 
 sprintf(command_string,"ls -tr %s", logs);
 ls_output = popen(command_string,"r");
 if (ls_output != NULL){
  len = fread(tlogs, sizeof(char), MAX_CHARS - 1 , ls_output);
  if (len>0){
   tlogs[len-1]='\000';
  }
  pclose(ls_output);
 
  free(command_string);
  free(datefile);
  free(touch_out);
  free(rm_out);
  free(logs);
  
  slogs[0]='\0';
  
  if((oplogs=malloc(10*STR_CHARS * sizeof *oplogs)) == 0){
     sysfatal("can't malloc oplogs: %r");
  }
  
  maxtok = strtoken(tlogs, '\n', oplogs);
  last_tag=maxtok;
  free(tlogs);
  
  for(i=0; i<maxtok; i++){
   strcat(slogs,oplogs[i]);
   strcat(slogs," ");
   free(oplogs[i]);
  }
  free(oplogs);

/* last_tag is used to see if there is only one log file and to avoid to rescan it*/

  if(last_tag==0){
   free(slogs);
   return NULL;
  }
  
  return slogs;
  
 } else {
 
  pclose(ls_output);
  free(command_string);
  free(datefile);
  free(touch_out);
  free(rm_out);
  free(logs);
  free(tlogs);
  free(slogs);
  return NULL;
  
 }
}

void CreamConnection(int c_sock){ 

    char      *buffer;
    int       retcod;
    
    struct   pollfd fds[2];   /*      poll file descp. struct	       */
    struct   pollfd *pfds;    /*      pointer to fds		       */
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
    char *notstrshort;
    char *notdate;
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
      notstrshort=iepoch2str(notepoch,"S");      
      
      if(cream_recycled){
       logepoch=nti[jcount+1];
      }else{
       if(nti[0]==0){
        logepoch=str2epoch(cnow,"D");
       }else{
         logepoch=nti[0];
       }
      }
      
      if(notepoch<=logepoch){
       GetEventsInOldLogs(notstrshort);
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
            
      for(ii=0;ii<jcount;ii++){
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
      sprintf(outreason," Reason=\"pbs_reason=%s\";" ,reason);
    }
    
    maxtok = strtoken(blahjobid, '_', clientjobid);    
    
    if(strcmp(wn,"NA")!=0){
      sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=\"%s\"; ClientJobId=\"%s\"; WorkerNode=%s;%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], wn, outreason, timestamp);
    }else{
      sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=\"%s\"; ClientJobId=\"%s\";%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], outreason, timestamp);
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

char *convdate(char *date){
  
 char *dateout;
 size_t max=100;

 struct tm *tm;
 if((tm=malloc(max)) == 0){
  sysfatal("can't malloc tm in convdate: %r");
 }

 strptime(date,"%m/%d/%Y %T",tm);
 
 dateout=malloc(max);
 
 strftime(dateout,max,"%Y-%m-%d %T",tm);
 free(tm);
 
 return dateout;
 
}

char *iepoch2str(int epoch, char * f){
  
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
 
 if(strcmp(f,"S")==0){
  strftime(dateout,max,"%Y%m%d",tm);
 }else if(strcmp(f,"L")==0){
  strftime(dateout,max,"%Y%m%d%H%M.%S",tm);
 }
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
 }else if(strcmp(f,"D")==0){
  strptime(str,"%Y%m%d",tm);
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

void print_usage(){

 fprintf(stderr,"Usage:\n");
 fprintf(stderr,"%s [-p] [<remote_port [%d]>] [-s <PBS_spooldir [%s]>] [-m  <CreamPort>] [-d <loglevel>] [-l <DebugLogFile> [%s]] [-D] [-v]\n",progname, DEFAULT_PORT, spooldir, debuglogname);
 fprintf(stderr,"-d\t\t enable debugging (1|2|3)\n");
 fprintf(stderr,"-l\t\t to specify a logfile (works only with -d)\n");
 fprintf(stderr,"-D\t\t to run as daemon.\n");
 fprintf(stderr,"-v\t\t print version\n");
 fprintf(stderr,"-h\t\t print this help\n");
 exit(EXIT_SUCCESS);
 
}

void print_version(){

 fprintf(stderr,"%s Version: %s\n",progname,VERSION);
 exit(EXIT_SUCCESS);
 
}

int ParseCmdLine(int argc, char *argv[], char **szPort, char **szSpoolDir, char **szCreamPort, char **szDebugLogName, char **szDebugLevel) {
    
    int n = 1;
     
    if(argc==2){
       if(!strncmp(argv[n], "-D", 2)){
          dmn=1;
          *szPort=NULL;
          return 0;
       }else if ( !strncmp(argv[n], "-h", 2)){
          print_usage();
       }else if ( !strncmp(argv[n], "-v", 2) ) {
          print_version();
       }else{
          *szPort= argv[n];
          return 0;
       }
    }

    while ( n < argc ) {
        if ( !strncmp(argv[n], "-p", 2) ) {
            *szPort= argv[++n];
        }else if ( !strncmp(argv[n], "-s", 2) ) {
            *szSpoolDir = argv[++n];
        }else if ( !strncmp(argv[n], "-m", 2) ) {
            *szCreamPort = argv[++n];
	    usecream++;
        }else if ( !strncmp(argv[n], "-l", 2) ) {
            *szDebugLogName = argv[++n];
        }else if ( !strncmp(argv[n], "-d", 2) ) {
	    *szDebugLevel = argv[++n];
        }else if ( !strncmp(argv[n], "-D", 2) ) {
	    dmn=1;
        }else if ( !strncmp(argv[n], "-v", 2) ) {
            print_version();
        }else if ( !strncmp(argv[n], "-h", 2) ) {
            print_usage(); 
	}else {
	    fprintf(stderr,"Wrong argument.\n");
            print_usage(); 
        }
        ++n;
    }
    
    return 0;
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
