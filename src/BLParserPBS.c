#include "BLParserPBS.h"

int 
main(int argc, char *argv[])
{

	struct    sockaddr_in servaddr;
	int       i,j;
	int       set = 1;
	int       status;
	int       list_s;
	int       list_c;
    
	char      *eventsfile;
    
	time_t now;
	struct tm *tptr;
   
	int version=0;
	const char *nport;

	pthread_t ReadThd[NUMTHRDS];
	pthread_t UpdateThd;
	pthread_t CreamThd;

	poptContext poptcon;
	int rc;			     
	struct poptOption poptopt[] = { 	
		{ "port",      'p', POPT_ARG_INT,    &port,         0, "port",            "<port number>"  },
		{ "creamport", 'm', POPT_ARG_INT,    &creamport,    0, "creamport",   "<creamport number>" },
		{ "spooldir",  's', POPT_ARG_STRING, &spooldir,     0, "PBS spooldir",     "<PBSspooldir>" },
		{ "logfile ",  'l', POPT_ARG_STRING, &debuglogname, 0, "DebugLogFile",    "<DebugLogFile>" },
		{ "debug",     'd', POPT_ARG_INT,    &debug,        0, "enable debugging",            NULL },
		{ "daemon",    'D', POPT_ARG_NONE,   &dmn,          0, "run as daemon",               NULL },
		{ "version",   'v', POPT_ARG_NONE,   &version,      0, "print version and exit",      NULL },
		POPT_AUTOHELP
	POPT_TABLEEND
	};
    
	char *espooldir;
	FILE      *fpt;

	argv0 = argv[0];

	/*Ignore sigpipe*/
    
	signal(SIGPIPE, SIG_IGN);             
        
	poptcon = poptGetContext(NULL, argc, (const char **) argv, poptopt, 0);
 
	if((rc = poptGetNextOpt(poptcon)) != -1){
		sysfatal("Invalid flag supplied: %r");
	}
	nport=poptGetArg(poptcon);
	
	if(version) {
		printf("%s Version: %s\n",progname,VERSION);
		exit(EXIT_SUCCESS);
	}
	
	if(debug <=0){
		debug=0;
	}
	
	if(debug){
		if((debuglogfile = fopen(debuglogname, "a+"))==0){
			debuglogfile =  fopen("/dev/null", "a+");
		}
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

	if(creamport){
		usecream=1;
	}
 
	/* Get log dir name */

	if((espooldir=getenv("PBS_SPOOL_DIR"))!=NULL){
		if((ldir=calloc(strlen(espooldir)+strlen("server_logs")+2,1)) == 0){
			sysfatal("can't malloc ldir: %r");
		}
		sprintf(ldir,"%s/server_logs",espooldir);
	
	} else{
		if((ldir=calloc(strlen(spooldir)+strlen("server_logs")+2,1)) == 0){
			sysfatal("can't malloc ldir: %r");
		}
		sprintf(ldir,"%s/server_logs",spooldir);
	}

	now=time(NULL);
	tptr=localtime(&now);
	strftime(cnow,sizeof(cnow),"%Y%m%d",tptr);

	if((eventsfile=calloc(strlen(ldir)+strlen(cnow)+2,1)) == 0){
		sysfatal("can't malloc eventsfile: %r");
	}
	sprintf(eventsfile,"%s/%s",ldir,cnow);
    
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
			if ( c == '\n' ) {
				break;
			}
		} else if ( rc == 0 ) {
			if ( n == 1 ) {
				return 0;
			} else {
				break;
			}
		} else {
			if ( errno == EINTR ) {
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
	long real_off = 0;

	char   *tdir;
	time_t lnow;
	struct tm *timeptr;
	char   *tnow;
	char   *evfile;
	char   *actualfile;
   
	if((tdir=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc tdir: %r");
	}
    
	if((evfile=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc evfile: %r");
	}

	if((tnow=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc tnow: %r");
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
		strftime(tnow,NUM_CHARS,"%Y%m%d",timeptr);
	
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
				sleep(1);
				continue;
			}

		}

		if(fseek(fp, 0L, SEEK_END) < 0){
			sysfatal("couldn't seek in follow: %r");
		}
		real_off=ftell(fp);
	
		if(real_off < off){
			off=real_off;
		}
   
		if(fseek(fp, off, SEEK_SET) < 0){
			sysfatal("couldn't seek in follow: %r");
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
		if((strstr(line,rex_queued)!=NULL) || (strstr(line,rex_running)!=NULL) || (strstr(line,rex_deleted)!=NULL) || (strstr(line,rex_finished)!=NULL) || (strstr(line,rex_hold)!=NULL)){
			if(debug >= 2){
				fprintf(debuglogfile, "Tail line:%s",line);
				fflush(debuglogfile);
			}
			AddToStruct(line,1);
		}
	}

	if((off=ftell(fp)) < 0){
		sysfatal("couldn't ftell in tail: %r");
	}
	return off;
}


int
InfoAdd(int id, char *value, const char * flag)
{

	char *jobid;
	unsigned h_blahjob;

        if(!value || (strlen(value)==0) || (strcmp(value,"\n")==0)){
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
 
		if((jobid=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc jobid: %r");
		}
		free(j2bl[id]);
		j2bl[id] = strdup(value);
  
		h_blahjob=hash(value);
		bjl[h_blahjob]=strdup(rfullptr[id]);
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
    
		return -1;
 
	}
	/* release write lock */
	pthread_mutex_unlock( &write_mutex );
    
	return 0;
}

int
AddToStruct(char *line, int flag)
{

	int has_blah=0;
 
	char *trex;
	char *rex;

	int  maxtok,ii; 
	char **tbuf;
 
	int id;
	int is_queued=0;
	int is_finished=0;

	char *tjobid=NULL;
	char *jobid=NULL;

	char *tj_time=NULL;
	char *j_time=NULL;

	char *tex_status=NULL;
	char *ex_status=NULL;

	char *tb_job=NULL;
	char *tj_blahjob=NULL;
	char *j_blahjob=NULL;
 
	if((strstr(line,blahjob_string)!=NULL) || (strstr(line,cream_string)!=NULL)){
		has_blah=1;
	}
 
	if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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

		if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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

	id=UpdatePtr(atoi(jobid),tjobid);
  
	/* get j_blahjob */

	if(rex && (strstr(rex,rex_queued)!=NULL)){
 		 is_queued=1; 

		if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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

		if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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

		if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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
  
		if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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
  
		if((tbuf=calloc(TBUFSIZE * sizeof *tbuf,1)) == 0){
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

char *
GetAllEvents(char *file)
{
 
	FILE *fp;
	char *line;
	char **opfile=NULL;
	int i=0;
	int maxtok;

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
				if((strstr(line,rex_queued)!=NULL) || (strstr(line,rex_running)!=NULL) || (strstr(line,rex_deleted)!=NULL) || (strstr(line,rex_finished)!=NULL) || (strstr(line,rex_hold)!=NULL)){
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
	char      *jstat;
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
			sysfatal("Error calling accept(): %r");
		}

		if((buffer=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc buffer in LookupAndSend: %r");
		}
		if((jstat=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc jstat in LookupAndSend: %r");
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
			sprintf(out_buf,"YPBS\n");
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
	
/* get jobid from blahjob id (needed by *_submit.sh) */
       
		if(strcmp(logdate,"BLAHJOB")==0){
			for(i=0;i<WRETRIES;i++){
				pthread_mutex_lock(&write_mutex);
				if(bjl[hash(jobid)]==NULL){
					pthread_mutex_unlock(&write_mutex);
					sleep(1);
					continue;
				}
				if((out_buf=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc out_buf in LookupAndSend: %r");
				}
				sprintf(out_buf,"%s\n",bjl[hash(jobid)]);
				pthread_mutex_unlock(&write_mutex);
				goto close;
			}
			if(i==WRETRIES){
				if((out_buf=calloc(STR_CHARS,1)) == 0){
					sysfatal("can't malloc out_buf in LookupAndSend: %r");
				}
				sprintf(out_buf,"\n",jobid);
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
			pthread_mutex_unlock(&write_mutex);
	 
		} else {
	
			pthread_mutex_unlock(&write_mutex);
			GetEventsInOldLogs(logdate);
	 
			id=GetRdxId(atoi(jobid));
	 
			pthread_mutex_lock(&write_mutex);
			if(id>0 && j2js[id]!=NULL){

				if((out_buf=calloc(STR_CHARS,1)) == 0){
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
		free(jstat);
	
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
			strcat(slogs,s);
			strcat(slogs," ");
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
        
		if(retcod < 0){
			close(conn_c);
			sysfatal("Poll error in CreamConnection: %r");
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
	char *notstrshort;
	char *notdate;
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
		sprintf(outreason," Reason=\"pbs_reason=%s\";" ,reason);
		if(strcmp(reason,"271")==0){
			sprintf(exitreason," ExitReason=\"Killed by Resource Management System\";");
		}
	}
    
	maxtok = strtoken(blahjobid, '_', clientjobid);    
    
	if(strcmp(wn,"NA")!=0){
		sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=\"%s\"; ClientJobId=\"%s\"; WorkerNode=%s;%s%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], wn, outreason, exitreason, timestamp);
	}else{
		sprintf(buffer,"[BatchJobId=\"%s\"; JobStatus=%s; BlahJobName=\"%s\"; ClientJobId=\"%s\";%s%s ChangeTime=\"%s\";]\n",sjobid, newstatus, blahjobid, clientjobid[1], outreason, exitreason, timestamp);
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
        
	if(retcod <0){
		close(conn_c);
		sysfatal("Poll error in NotifyCream: %r");
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
UpdatePtr(int jid,char *fulljobid)
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
		rptr[ptrcnt]=jid;
		if(recycled){
			free(rfullptr[ptrcnt]);
		}
		rfullptr[ptrcnt++]=strdup(fulljobid);
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
		sysfatal("can't malloc tmp in strtoken: %r");
	}
	assert(tmp);
	strcpy(tmp, s);
	ptr = tmp;
	while(1) {
		if((dptr = strchr(ptr, delim)) != NULL) {
			*dptr = '\0';
			if((token[i] = calloc(1 + strlen(ptr),1)) == 0){
				sysfatal("can't malloc token[i] in strtoken: %r");
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
					sysfatal("can't malloc token[i] in strtoken: %r");
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
convdate(char *date)
{
  
	char *dateout;

	struct tm *tm;
	
	if((tm=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc tm in convdate: %r");
	}

	strptime(date,"%m/%d/%Y %T",tm);
 
	if((dateout=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc dateout in convdate: %r");
	}
 
	strftime(dateout,NUM_CHARS,"%Y-%m-%d %T",tm);
	free(tm);
 
	return dateout;
 
}

char *
iepoch2str(int epoch, char * f)
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
 
	if(strcmp(f,"S")==0){
		strftime(dateout,NUM_CHARS,"%Y%m%d",tm);
	}else if(strcmp(f,"L")==0){
		strftime(dateout,NUM_CHARS,"%Y%m%d%H%M.%S",tm);
	}
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
	}else if(strcmp(f,"D")==0){
		strptime(str,"%Y%m%d",tm);
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
