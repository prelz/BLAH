#include "BLParserLSF.h"

int
main(int argc, char *argv[])
{

	struct    sockaddr_in servaddr;
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
    
	if(debug){
		if((debuglogfile = fopen(debuglogname, "a+"))==0){
			debuglogfile =  fopen("/dev/null", "a+");
		}
	}
    
	if((eventsfile=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc eventsfile: %r");
	}
    
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
		sysfatal("Error creating listening socket: %r");
	}

	if(setsockopt(list_s, SOL_SOCKET, SO_REUSEADDR, &set, sizeof(set)) < 0) {
		syserror("setsockopt() failed: %r");
	}

	memset(&servaddr, 0, sizeof(servaddr));
	servaddr.sin_family      = AF_INET;
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr.sin_port        = htons(port);

	if ( bind(list_s, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0 ) {
		sysfatal("Error calling bind() in main: %r");
	}
    
	if ( listen(list_s, LISTENQ) < 0 ) {
		sysfatal("Error calling listen() in main: %r");
	}
    
	/* create listening socket for Cream */
    
	if(usecream>0){
      
		if ( !creamport ) {
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
		cservaddr.sin_port	= htons(creamport);

		if ( bind(list_c, (struct sockaddr *) &cservaddr, sizeof(cservaddr)) < 0 ) {
			sysfatal("Error calling bind() in main: %r");
		}
      
		if ( listen(list_c, LISTENQ) < 0 ) {
			sysfatal("Error calling listen() in main: %r");
		}
	}
    
	if(dmn){
		daemonize();
	}
   
    
	for(i=0;i<NUMTHRDS;i++){
		pthread_create(&ReadThd[i], NULL, (void *)LookupAndSend, (void *)list_s);
	}
    
	if(usecream>0){
		pthread_create(&CreamThd, NULL, (void *)CreamConnection, (void *)list_c);
	}

	pthread_create(&UpdateThd, NULL, mytail, (void *)eventsfile);
	pthread_join(UpdateThd, (void **)&status);
    
	pthread_exit(NULL);
 
}

/*---Functions---*/

ssize_t
Readline(int sockd, void *vptr, size_t maxlen)
{
	ssize_t n, rc;
	char    c, *buffer;

	buffer = vptr;

	for ( n = 1; n < maxlen; n++ ) {
	
		if ( (rc = read(sockd, &c, 1)) == 1 ) {
			*buffer++ = c;
			if ( c == '\n' ){
				break;
			}
		} else if ( rc == 0 ) {
			if ( n == 1 ) {
				return 0;
			} else {
				break;
			}
		} else {
			if ( errno == EINTR ){
				continue;
			}
			return -1;
		}
	}

	*buffer = 0;
	return n;
}

ssize_t
Writeline(int sockd, const void *vptr, size_t n)
{
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

unsigned
hash(char *s)
{

	unsigned hashval;

	for(hashval = 0; *s!='\0';s++){
		hashval = *s + 31 *hashval;
	}
	return hashval % RDXHASHSIZE;
}


void *
mytail (void *infile)
{    
        
	char *linebuffer;
    
	if((linebuffer=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc linebuffer: %r");
	}
    
	follow((char *)infile, linebuffer);
   
	return 0;
}

void
follow(char *infile, char *line)
{
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

long
tail(FILE *fp, char *line)
{
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


int
InfoAdd(int id, char *value, const char * flag)
{

	if(!value){
		return -1;
	}

	if(debug){
		fprintf(debuglogfile, "Adding: ID:%d Type:%s Value:%s\n",rptr[id],flag,value);
		fflush(debuglogfile);
	} 
	/* set write lock */
	pthread_mutex_lock( &write_mutex );
 
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

		return -1;
 
	}
	/* release write lock */
	pthread_mutex_unlock( &write_mutex );

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
 
	if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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

char *
GetAllEvents(char *file)
{
 
	FILE *fp;
	char *line;
	char **opfile;
	int maxtok,i;

	if((opfile=calloc(STR_CHARS * sizeof *opfile,1)) == 0){
		sysfatal("can't malloc opfile: %r");
	}
 
	maxtok = strtoken(file, ' ', opfile);

	if((line=calloc(STR_CHARS,1)) == 0){
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
			sysfatal("Cannot open %s file: %r",opfile[i]);
		}
  
		fclose(fp);
		free(opfile[i]);

	} /* close for*/
	free(file);
	free(line);
	free(opfile);
    
	return NULL;

}

void *
LookupAndSend(int m_sock)
{ 
    
	char      *buffer;
	char      *out_buf;
	char      *logdate;
	char      *jobid;
	char      *h_jobid;
	char      *t_wnode;
	char      *exitreason;
	char      *pr_removal="Not";
	int       i,maxtok,ii;
	int       id;
	int       conn_s;
	char      **tbuf;
	char      *cp;
	char      *irptr;
	int       listcnt=0;
	int       listbeg=0;
    
	while ( 1 ) {
	
		/*  Wait for a connection, then accept() it  */
	
		if ( (conn_s = accept(m_sock, NULL, NULL) ) < 0 ) {
			sysfatal("Error calling accept() in LookupAndSend: %r");
		}

		if((buffer=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc buffer in LookupAndSend: %r");
		}

		if((h_jobid=calloc(NUM_CHARS,1)) == 0){
			sysfatal("can't malloc h_jobid in LookupAndSend: %r");
		}
	
		Readline(conn_s, buffer, STR_CHARS-1);	
		if(debug){
			fprintf(debuglogfile, "Received:%s",buffer);
			fflush(debuglogfile);
		}
	
		/* printf("thread/0x%08lx\n",pthread_self()); */
	
		if((strlen(buffer)==0) || (strcmp(buffer,"\n")==0) || (strstr(buffer,"/")==0) || (strcmp(buffer,"/")==0)){
         
			if((logdate=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc logdate in LookupAndSend: %r");
			}
			if((jobid=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc jobid in LookupAndSend: %r");
			}
			if((out_buf=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			sprintf(out_buf,"\n");

			goto close;
		}
	        
		if ((cp = strrchr (buffer, '\n')) != NULL){
			*cp = '\0';
		}

		if((tbuf=calloc(10 * sizeof *tbuf,1)) == 0){
			sysfatal("can't malloc tbuf: %r");
		}
	
		maxtok=strtoken(buffer,'/',tbuf);
		if(tbuf[0]){
			logdate=strdup(tbuf[0]);
		}else{
			if((logdate=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc logdate in LookupAndSend: %r");
			}
		}
		if(tbuf[1]){
			jobid=strdup(tbuf[1]);
		}else{
			if((jobid=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc jobid in LookupAndSend: %r");
			}
		}

		for(ii=0;ii<maxtok;ii++){
			free(tbuf[ii]);
		}
		free(tbuf);		

/* HELP reply */
       
		if(strcmp(logdate,"HELP")==0){
			if((out_buf=calloc(MAX_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			sprintf(out_buf,"Commands: BLAHJOB/<blahjob-id> <date-YYYYmmdd>/<jobid> HELP TEST VERSION CREAMPORT TOTAL LISTALL LISTF[/<first-n-jobid>] LISTL[/<last-n-jobid>]\n");
			goto close;
		}

/* TEST reply */
       
		if(strcmp(logdate,"TEST")==0){
			if((out_buf=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			sprintf(out_buf,"YLSF\n");
			goto close;
		}

/* VERSION reply */
       
		if(strcmp(logdate,"VERSION")==0){
			if((out_buf=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			sprintf(out_buf,"%s\n",VERSION);
			goto close;
		}
	
/* TOTAL reply */
       
		if(strcmp(logdate,"TOTAL")==0){
			if((out_buf=calloc(MAX_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			if(recycled){
				sprintf(out_buf,"Total number of jobs:%d\n",RDXHASHSIZE);
			}else{
				sprintf(out_buf,"Total number of jobs:%d\n",ptrcnt-1);
			}
			goto close;
		}
	
/* LISTALL reply */

		if(strcmp(logdate,"LISTALL")==0){
			if((out_buf=calloc(MAX_CHARS*3,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			if((irptr=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc irptr in LookupAndSend: %r");
			}
			if(recycled){
				for(i=ptrcnt;i<RDXHASHSIZE;i++){
					sprintf(irptr,"%d",rptr[i]);
					strcat(out_buf,irptr);
					strcat(out_buf," ");
				}
			}
			for(i=1;i<ptrcnt;i++){
				sprintf(irptr,"%d",rptr[i]);
				strcat(out_buf,irptr);
				strcat(out_buf," ");
			}
			free(irptr);
			strcat(out_buf,"\n");
			goto close;
		}

/* LISTF reply */
       
		if(strcmp(logdate,"LISTF")==0){
			if((out_buf=calloc(MAX_CHARS*3,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
	 
			if((listcnt=atoi(jobid))<=0){
				listcnt=10;
			}
			if(listcnt>ptrcnt-1){
				listcnt=ptrcnt-1;
			}
			if((irptr=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc irptr in LookupAndSend: %r");
			}
			sprintf(out_buf,"List of first %d jobid:",listcnt);
			if(recycled){
				for(i=ptrcnt;i<ptrcnt+listcnt;i++){
					sprintf(irptr,"%d",rptr[i]);
					strcat(out_buf,irptr);
					strcat(out_buf," ");
				}
			}else{
				for(i=1;i<=listcnt;i++){
					sprintf(irptr,"%d",rptr[i]);
					strcat(out_buf,irptr);
					strcat(out_buf," ");
				}
			}
			free(irptr);
			strcat(out_buf,"\n");
			goto close;
		}

/* LISTL reply */
       
		if(strcmp(logdate,"LISTL")==0){
			if((out_buf=calloc(MAX_CHARS*3,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
	 
			if((listcnt=atoi(jobid))<=0){
				listcnt=10;
			}
			if(ptrcnt-listcnt>0){
				listbeg=ptrcnt-listcnt;
			}else{
				listbeg=1;
				listcnt=ptrcnt-1;
			}
	 
			if((irptr=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc irptr in LookupAndSend: %r");
			}
			sprintf(out_buf,"List of latest %d jobid:",listcnt);
	 
			if(recycled){
				for(i=RDXHASHSIZE+(ptrcnt-listcnt);i<RDXHASHSIZE;i++){
					sprintf(irptr,"%d",rptr[i]);
					strcat(out_buf,irptr);
					strcat(out_buf," ");
				}
			}
			for(i=listbeg;i<ptrcnt;i++){
				sprintf(irptr,"%d",rptr[i]);
				strcat(out_buf,irptr);
				strcat(out_buf," ");
			}
			free(irptr);
			strcat(out_buf,"\n");
			goto close;
		}

/* get port where the parser is waiting for a connection from cream and send it to cream */
       
		if(strcmp(logdate,"CREAMPORT")==0){
			if((out_buf=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			sprintf(out_buf,"%d\n",creamport);
			goto close;
		}
	
/* get jobid from blahjob id (needed by lsf_submit.sh) */
	
		if(strcmp(logdate,"BLAHJOB")==0){
			for(i=0;i<WRETRIES;i++){
				h_jobid[0]='\0';
				strcat(h_jobid,"\"");
				strcat(h_jobid,jobid);
				strcat(h_jobid,"\"");
				pthread_mutex_lock(&write_mutex);
				if(bjl[hash(h_jobid)]==NULL){
					pthread_mutex_unlock(&write_mutex);
					sleep(1);
					continue;
				}
				if((out_buf=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc out_buf in LookupAndSend: %r");
				}
				sprintf(out_buf,"%s\n",bjl[hash(h_jobid)]);
				pthread_mutex_unlock(&write_mutex);
				goto close;
			}
			if(i==WRETRIES){

				if((out_buf=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc out_buf in LookupAndSend: %r");
				}
				sprintf(out_buf,"\n");

				goto close;
			}
		}

		if((strlen(logdate)==0) || (strcmp(logdate,"\n")==0)){
			if((out_buf=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			sprintf(out_buf,"\n");

			goto close;

		}
	
/* get all info from jobid */
     
		id=GetRdxId(atoi(jobid));

		pthread_mutex_lock(&write_mutex);
	
		if(id>0 && j2js[id]!=NULL){
         
			if((out_buf=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc out_buf in LookupAndSend: %r");
			}
			if((t_wnode=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc t_wnode in LookupAndSend: %r");
			}
			if((exitreason=calloc(STR_CHARS,1)) == 0){
				sysfatal("can't malloc exitreason in LookupAndSend: %r");
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
				if((strcmp(j2ec[id],"130")==0) || (strcmp(j2ec[id],"137")==0) || (strcmp(j2ec[id],"143")==0)){
					sprintf(exitreason," ExitReason=\"Memory limit reached\";");
				}else if(strcmp(j2ec[id],"140")==0){
					sprintf(exitreason," ExitReason=\"RUNtime limit reached\";");
				}else if(strcmp(j2ec[id],"152")==0){
					sprintf(exitreason," ExitReason=\"CPUtime limit reached\";");
				}else if(strcmp(j2ec[id],"153")==0){
					sprintf(exitreason," ExitReason=\"FILEsize limit reached\";");
				}else if(strcmp(j2ec[id],"157")==0){
					sprintf(exitreason," ExitReason=\"Directory Access Error (No AFS token, dir does not exist)\";");
				}
				sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\"; LRMSCompletedTime=\"%s\";%s ExitCode=%s;]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], j2ct[id], exitreason, j2ec[id], pr_removal);
			}else if(strcmp(j2rt[id],"\0")!=0){
				sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], pr_removal);
			}else{
				sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], pr_removal);
			}
			pthread_mutex_unlock(&write_mutex);

			free(t_wnode);
			free(exitreason);
		} else {
        
			pthread_mutex_unlock(&write_mutex);
			GetEventsInOldLogs(logdate);
         
			id=GetRdxId(atoi(jobid));

			pthread_mutex_lock(&write_mutex);
			if(id>0 && j2js[id]!=NULL){

				if((out_buf=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc out_buf in LookupAndSend: %r");
				}
				if((t_wnode=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc t_wnode in LookupAndSend: %r");
				}
				if((exitreason=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc exitreason in LookupAndSend: %r");
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
					if((strcmp(j2ec[id],"130")==0) || (strcmp(j2ec[id],"137")==0) || (strcmp(j2ec[id],"143")==0)){
						sprintf(exitreason," ExitReason=\"Memory limit reached\";");
					}else if(strcmp(j2ec[id],"140")==0){
						sprintf(exitreason," ExitReason=\"RUNtime limit reached\";");
					}else if(strcmp(j2ec[id],"152")==0){
						sprintf(exitreason," ExitReason=\"CPUtime limit reached\";");
					}else if(strcmp(j2ec[id],"153")==0){
						sprintf(exitreason," ExitReason=\"FILEsize limit reached\";");
					}else if(strcmp(j2ec[id],"157")==0){
						sprintf(exitreason," ExitReason=\"Directory Access Error (No AFS token, dir does not exist)\";");
					}
					sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\"; LRMSCompletedTime=\"%s\";%s ExitCode=%s;]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], j2ct[id], exitreason, j2ec[id], pr_removal);
				}else if(strcmp(j2rt[id],"\0")!=0){
					sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\"; LRMSStartRunningTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], j2rt[id], pr_removal);
				}else{
					sprintf(out_buf,"[BatchJobId=\"%s\"; %s JobStatus=%s; LRMSSubmissionTime=\"%s\";]/%s\n",jobid, t_wnode, j2js[id], j2st[id], pr_removal);
				}
				free(t_wnode);
				free(exitreason);
				pthread_mutex_unlock(&write_mutex);
           
			} else {
				pthread_mutex_unlock(&write_mutex);
				if((out_buf=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc out_buf in LookupAndSend: %r");
				}
				sprintf(out_buf,"JobId %s not found/Not\n",jobid);
			}
         
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
			sysfatal("Error calling close(): %r");
		}
	
	} /* closes while */
	
	return 0; 
}

int
GetEventsInOldLogs(char *logdate)
{

	char *loglist=NULL;
 
	loglist=GetLogList(logdate);
 
	if(loglist!=NULL){
		GetAllEvents(loglist);
	}
 
	return 0;
 
}

char *
GetLogDir(int largc, char *largv[])
{

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

	poptContext poptcon;
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

	if((line=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc line: %r");
	}
	if((logpath=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc logpath: %r");
	}
	if((lsf_clustername=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc lsf_clustername: %r");
	}
	if((command_string=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc command_string: %r");
	}
	if((ls_out=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc ls_out: %r");
	}
	if((conffile=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc conffile: %r");
	}

	if((tbuf=calloc(10 * sizeof *tbuf,1)) == 0){
		sysfatal("can't malloc tbuf: %r");
	}
	
	poptcon = poptGetContext(NULL, largc, (const char **) largv, poptopt, 0);
 
	if((rc = poptGetNextOpt(poptcon)) != -1){
		sysfatal("Invalid flag supplied: %r");
	}
	nport=poptGetArg(poptcon);
 
	if(version) {
		printf("%s Version: %s\n",progname,VERSION);
		exit(EXIT_SUCCESS);
	}   
	if(port) {
		if ( port < 1 || port > 65535) {
			sysfatal("Invalid port supplied: %r");
		}
	}else if(nport){
		port=atoi(nport);
		if ( port < 1 || port > 65535) {
			sysfatal("Invalid port supplied: %r");
		}
	}else{
		port=DEFAULT_PORT;
	}   

	if ( creamport > 0 && port <= 65535 ) {
		usecream=1;
	}

	if(debug <=0){
		debug=0;
	}
  
	if((econfpath=getenv("LSF_ENVDIR"))!=NULL){
		sprintf(conffile,"%s/lsf.conf",econfpath);
		if((fp=fopen(conffile, "r")) != 0){
			while(fgets(line, STR_CHARS, fp)){
				if(strstr(line,"LSB_SHAREDIR")!=0){
					goto creamdone;
				}
			}
		}
	}
	
	sprintf(conffile,"%s/lsf.conf",confpath);
	
	if((fp=fopen(conffile, "r")) != 0){
		while(fgets(line, STR_CHARS, fp)){
			if(strstr(line,"LSB_SHAREDIR")!=0){
				goto creamdone;
			}
		}
	}
	
	if((econfpath=getenv("LSF_CONF_PATH"))!=NULL){
		sprintf(conffile,"%s/lsf.conf",econfpath);
		if((fp=fopen(conffile, "r")) != 0){
			while(fgets(line, STR_CHARS, fp)){
				if(strstr(line,"LSB_SHAREDIR")!=0){
					goto creamdone;
				}
			}
		}
	}

creamdone:
	maxtok=strtoken(line,'=',tbuf);
	if(tbuf[1]){
		lsf_base_pathtmp=strdup(tbuf[1]);
	} else {
		sysfatal("Unable to find logdir in conf file: %r");
	}
 
	if ((cp = strrchr (lsf_base_pathtmp, '\n')) != NULL){
		*cp = '\0';
	}
 
	lsf_base_path=strdel(lsf_base_pathtmp, "\" ");
	free(lsf_base_pathtmp);
 
	if((ebinpath=getenv("LSF_BINDIR"))!=NULL){
 
		if((s=calloc(strlen(ebinpath)+strlen("lsid")+2,1)) == 0){
			sysfatal("can't malloc s: %r");
		}
		sprintf(s,"%s/lsid",ebinpath);
		rc=stat(s,&sbuf);
		if(rc) {
			sysfatal("%s not found: %r",s);
		}
		if( ! (sbuf.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ) {
			sysfatal("%s is not executable, but mode %05o: %r",s,(int)sbuf.st_mode);
		}
		free(s);
		sprintf(command_string,"%s/lsid | grep 'My cluster name is'|awk -F\" \" '{ print $5 }'",ebinpath);  
		goto bdone;
	}
 
	if((s=calloc(strlen(binpath)+strlen("lsid")+2,1)) == 0){
		sysfatal("can't malloc s: %r");
	}
	sprintf(s,"%s/lsid",binpath);
	rc=stat(s,&sbuf);
	if(rc) {
		sysfatal("%s not found: %r",s);
	}
	if( ! (sbuf.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ) {
		sysfatal("%s is not executable, but mode %05o: %r",s,(int)sbuf.st_mode);
	}
	free(s);
	sprintf(command_string,"%s/lsid | grep 'My cluster name is'|awk -F\" \" '{ print $5 }'",binpath);  
	goto bdone;
 
	if((ebinpath=getenv("LSF_BIN_PATH"))!=NULL){

		if((s=calloc(strlen(ebinpath)+strlen("lsid")+2,1)) == 0){
			sysfatal("can't malloc s: %r");
		}
		sprintf(s,"%s/lsid",ebinpath);
		rc=stat(s,&sbuf);
		if(rc) {
			sysfatal("%s not found: %r",s);
		}
		if( ! (sbuf.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ) {
			sysfatal("%s is not executable, but mode %05o: %r",s,(int)sbuf.st_mode);
		}
		free(s);
		sprintf(command_string,"%s/lsid | grep 'My cluster name is'|awk -F\" \" '{ print $5 }'",ebinpath);  
		goto bdone;
	}
 
bdone:
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

char *
GetLogList(char *logdate)
{
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
		syserror("Timestring \"%s\" is invalid (YYYYmmdd): %r", logdate);
		return NULL;
	}
	tage=mktime(&tmthr);

	if( !(dirh=opendir(ldir)) ) {
		syserror("Cannot open directory %s: %r", ldir);
		return NULL;
	}

	while ( (direntry=readdir(dirh)) ) {
		if( *(direntry->d_name) == '.' ) continue;
		if((s=calloc(strlen(direntry->d_name)+strlen(ldir)+2,1)) == 0){
			sysfatal("can't malloc s: %r");
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
	  
		retcod = poll(pfds, nfds, timeout); 
        
		if(retcod <0){
			close(conn_c);
			sysfatal("Poll error for Cream: %r");
		}
    
		if ( retcod > 0 ){
			if ( ( fds[0].revents & ( POLLERR | POLLNVAL | POLLHUP) )){
				switch (fds[0].revents){
				case POLLNVAL:
					syserror("poll() file descriptor error for Cream: %r");
					break;
				case POLLHUP:
					syserror("Connection closed for Cream: %r");
					break;
				case POLLERR:
					syserror("poll() POLLERR for Cream: %r");
					break;
				}
			} else {
            
				if ( (conn_c = accept(c_sock, NULL, NULL) ) < 0 ) {
					sysfatal("Error calling accept(): %r");
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

int
NotifyFromDate(char *in_buf)
{

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

	if((out_buf=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc out_buf: %r");
	}
    
	if((tbuf=calloc(10 * sizeof *tbuf,1)) == 0){
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

int
NotifyCream(int jobid, char *newstatus, char *blahjobid, char *wn, char *reason, char *timestamp, int flag)
{

	/*if flag ==0 AddToStruct is called within GetOldLogs 
	  if flag ==1 AddToStruct is called elsewhere*/
   
	char     *buffer;
	char     *outreason;
	char     *exitreason;
	char     *sjobid;
  
	int      retcod;
        
	struct   pollfd fds[2];
	struct   pollfd *pfds;
	int      nfds = 1;
	int      timeout= 1;
    
	char    **clientjobid;
	int      maxtok,i;
    
	fds[0].fd = conn_c;
	fds[0].events = 0;
	fds[0].events = ( POLLIN | POLLOUT | POLLPRI | POLLERR | POLLHUP | POLLNVAL ) ;
	pfds = fds;
    
    
	if((buffer=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc buffer: %r");
	}
	if((outreason=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc outreason: %r");
	}
	if((exitreason=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc exitreason: %r");
	}
	if((clientjobid=calloc(10 * sizeof *clientjobid,1)) == 0){
		sysfatal("can't malloc clientjobid %r");
	}
	if((sjobid=calloc(10 * sizeof *sjobid,1)) == 0){
		sysfatal("can't malloc sjobid %r");
	}
    
	sprintf(sjobid, "%d",rptr[jobid]);
    
	if(strcmp(reason,"NA")!=0){
		sprintf(outreason," Reason=\"lsf_reason=%s\";" ,reason);
		if((strcmp(reason,"130")==0) || (strcmp(reason,"137")==0) || (strcmp(reason,"143")==0)){
			sprintf(exitreason," ExitReason=\"Memory limit reached\";");
		}else if(strcmp(reason,"140")==0){
			sprintf(exitreason," ExitReason=\"RUNtime limit reached\";");
		}else if(strcmp(reason,"152")==0){
			sprintf(exitreason," ExitReason=\"CPUtime limit reached\";");
		}else if(strcmp(reason,"153")==0){
			sprintf(exitreason," ExitReason=\"FILEsize limit reached\";");
		}else if(strcmp(reason,"157")==0){
			sprintf(exitreason," ExitReason=\"Directory Access Error (No AFS token, dir does not exist)\";");
		}
	}
    
	maxtok = strtoken(blahjobid, '_', clientjobid);    
    
	if(strcmp(wn,"NA")!=0){
		sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=%s; ClientJobId=\"%s; WorkerNode=%s;%s%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], wn, outreason, exitreason, timestamp);
	}else{
		sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=%s; ClientJobId=\"%s;%s%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], outreason, exitreason, timestamp);
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
		free(exitreason);
		return -1;
	}
    
    
	retcod = poll(pfds, nfds, timeout); 
        
	if(retcod <=0){
		close(conn_c);
		sysfatal("Poll error for Cream: %r");
	}
    
	if ( retcod > 0 ){
		if ( ( fds[0].revents & ( POLLERR | POLLNVAL | POLLHUP) )){
			switch (fds[0].revents){
			case POLLNVAL:
				syserror("poll() file descriptor error for Cream: %r");
				break;
			case POLLHUP:
				syserror("Connection closed for Cream: %r");
				break;
			case POLLERR:
				syserror("poll() POLLERR for Cream: %r");
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
	free(exitreason);
    
	return 0;

}

int
UpdatePtr(int jid)
{

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

int
GetRdxId(int cnt)
{
	int i;
	for(i=0;i<RDXHASHSIZE;i++){
		if(rptr[i] == cnt){
			return i;
		}
	}
	return -1;
}

int
strtoken(const char *s, char delim, char **token)
{
	char *tmp;
	char *ptr, *dptr;
	int i = 0;
    
	if((tmp = calloc(1 + strlen(s),1)) == 0){
		sysfatal("can't malloc tmp: %r");
	}
	assert(tmp);
	strcpy(tmp, s);
	ptr = tmp;
	while(1) {
		if((dptr = strchr(ptr, delim)) != NULL) {
			*dptr = '\0';
			if((token[i] = calloc(1 + strlen(ptr),1)) == 0){
				sysfatal("can't malloc token[i]: %r");
			}
			assert(token[i]);
			strcpy(token[i], ptr);
			ptr = dptr + 1;
			if (strlen(token[i]) != 0){
				i++;
			}
		} else {
			if(strlen(ptr)) {
				if((token[i] = calloc(1 + strlen(ptr),1)) == 0){
					sysfatal("can't malloc token[i]: %r");
				}
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

char *
strdel(char *s, const char *delete)
{
	char *tmp, *cptr, *sptr;
    
	if(!delete || !strlen(delete)){
		return s;
	}
        
	if(!s || !strlen(s)){
		return s;
	}
        
	tmp = strndup(s, STR_CHARS);
       
	assert(tmp);
    
	for(sptr = tmp; (cptr = strpbrk(sptr, delete)); sptr = tmp) {
		*cptr = '\0';
		strcat(tmp, ++cptr);
	}
    
	return tmp;
}

char *
epoch2str(char *epoch)
{
  
	char *dateout;

	struct tm *tm;
	if((tm=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc tm in epoch2str: %r");
	}

	strptime(epoch,"%s",tm);
 
	if((dateout=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc dateout in epoch2str: %r");
	}
 
	strftime(dateout,NUM_CHARS,"%Y-%m-%d %T",tm);
	free(tm);
 
	return dateout;
 
}

char *
iepoch2str(int epoch)
{
  
	char *dateout;
	char *lepoch;

	struct tm *tm;
	
	if((tm=calloc(NUM_CHARS,1)) == 0){
	sysfatal("can't malloc tm in iepoch2str: %r");
	}
	if((lepoch=calloc(STR_CHARS,1)) == 0){
	sysfatal("can't malloc lepoch in iepoch2str: %r");
	}
 
	sprintf(lepoch,"%d",epoch);
 
	strptime(lepoch,"%s",tm);
 
	if((dateout=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc dateout in iepoch2str: %r");
	}
 
	strftime(dateout,NUM_CHARS,"%Y%m%d%H%M.%S",tm);
	free(tm);
	free(lepoch);
 
	return dateout;
 
}

int
str2epoch(char *str, char * f)
{
  
	char *dateout;
	int idate;

	struct tm *tm;
	
	if((tm=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc tm in str2epoch: %r");
	}
	if(strcmp(f,"S")==0){
		strptime(str,"%Y-%m-%d %T",tm);
	}else if(strcmp(f,"L")==0){
		strptime(str,"%a %b %d %T %Y",tm);
	}
 
	if((dateout=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc dateout in str2epoch: %r");
	}
 
	strftime(dateout,NUM_CHARS,"%s",tm);
 
	free(tm);
 
	idate=atoi(dateout);
	free(dateout);
 
	return idate;
 
}

void
daemonize()
{

	int pid;
    
	pid = fork();
	
	if (pid < 0){
		sysfatal("Cannot fork in daemonize: %r");
	}else if (pid >0){
		exit(EXIT_SUCCESS);
	}
    
	setsid();
    
	pid = fork();
	
	if (pid < 0){
		sysfatal("Cannot fork in daemonize: %r");
	}else if (pid >0){
		exit(EXIT_SUCCESS);
	}
	chdir("/");
	umask(0);
    
	freopen ("/dev/null", "r", stdin);  
	freopen ("/dev/null", "w", stdout);
	freopen ("/dev/null", "w", stderr); 

}

void
eprint(int err, char *fmt, va_list args)
{
	extern int errno;

	fprintf(stderr, "%s: ", argv0);
	if(fmt){
		vfprintf(stderr, fmt, args);
	}
	if(err){
		fprintf(stderr, "%s", strerror(errno));
	}
	fputs("\n", stderr);
	errno = 0;
}

char *
chopfmt(char *fmt)
{
	static char errstr[ERRMAX];
	char *p;

	errstr[0] = '\0';
	if((p=strstr(fmt, "%r")) != 0){
		fmt = strncat(errstr, fmt, p-fmt);
	}
	return fmt;
}

/* syserror: print error and continue */
void
syserror(char *fmt, ...)
{
	va_list args;
	char *xfmt;

	va_start(args, fmt);
	xfmt = chopfmt(fmt);
	eprint(xfmt!=fmt, xfmt, args);
	va_end(args);
}

/* sysfatal: print error and die */
void
sysfatal(char *fmt, ...)
{
	va_list args;
	char *xfmt;

	va_start(args, fmt);
	xfmt = chopfmt(fmt);
	eprint(xfmt!=fmt, xfmt, args);
	va_end(args);
	exit(EXIT_FAILURE);
}
