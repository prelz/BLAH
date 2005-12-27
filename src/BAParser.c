#include "BAParser.h"

int main(int argc, char *argv[]) {

    struct    sockaddr_in servaddr;
    char      *endptr;
    int       i;
    int       set = 1;
    int       list_s;
    
    char *szPort;
    char *szSpoolDir;

    pthread_t ReadThd[NUMTHRDS];

    argv0 = argv[0];

    /*Ignore sigpipe*/
    
    signal(SIGPIPE, SIG_IGN);
    
    ParseCmdLine(argc, argv, &szPort, &szSpoolDir);

    if(dmn){
     daemonize();
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

    if(szSpoolDir!=NULL){
     spooldir=szSpoolDir;
    }
    
    /*  Create the listening socket  */

    if ( (list_s = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
	fprintf(stderr, "%s: Error creating listening socket.\n",progname);
	exit(EXIT_FAILURE);
    }

    if(setsockopt(list_s, SOL_SOCKET, SO_REUSEADDR, &set, sizeof(set)) < 0) {
        fprintf(stderr,"%s: setsockopt() failed\n",progname);
    }

    /*  Set all bytes in socket address structure to
        zero, and fill in the relevant data members   */

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port        = htons(port);


    /*  Bind our socket addresss to the 
	listening socket, and call listen()  */

    if ( bind(list_s, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0 ) {
	fprintf(stderr, "%s: Error calling bind()\n",progname);
	exit(EXIT_FAILURE);
    }
    
    if ( listen(list_s, LISTENQ) < 0 ) {
    	fprintf(stderr, "%s: Error calling listen()\n",progname);
    	exit(EXIT_FAILURE);
    }
       
    for(i=0;i<NUMTHRDS;i++){
     pthread_create(&ReadThd[i], NULL, GetAndSend, (void *)list_s);
    }
    
   pthread_exit(NULL);
 
}

/*---functions---*/

/*  Read a line from a socket  */

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

/*  Write a line to a socket  */

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

void *GetAndSend(int m_sock){ 
    
    char      *buffer;
    char      *out_buf;
    int       conn_s;
    char      *cmd;
    char      *fulljobid;
    char      *jobid;
    char      *lrms;
    int       maxtok,ii;
    char      **tbuf;
    char      *cp;
    char      *dgas_out;
    
    while ( 1 ) {
    

	/*  Wait for a connection, then accept() it  */
	
	if ( (conn_s = accept(m_sock, NULL, NULL) ) < 0 ) {
	    fprintf(stderr, "%s: Error calling accept()\n",progname);
	    exit(EXIT_FAILURE);
	}
    
        if ((credential_handle = acquire_cred(GSS_C_ACCEPT)) == GSS_C_NO_CREDENTIAL)
        {
        	fprintf(stderr, "Unable to acquire credentials, exiting...\n");
        	exit(EXIT_FAILURE);
        }
        if ((context_handle = accept_context(credential_handle, &client_name, conn_s)) == GSS_C_NO_CONTEXT)
        {
 	        sprintf(out_buf,"\n");
		goto close;
        }
        else
        {
	 	
 	 if((buffer=malloc(STR_CHARS)) == 0){
 	   sysfatal("can't malloc buffer in GetAndSend: %r");
 	 }
  	 buffer[0]='\0';
 	 if((out_buf=malloc(STR_CHARS)) == 0){
 	   sysfatal("can't malloc out_buf in GetAndSend: %r");
 	 }
  	 out_buf[0]='\0';

  	 /* read line from socket */
 	 Readline(conn_s, buffer, STR_CHARS-1);
 	 if(debug){
 	  fprintf(stderr, "Received:%s",buffer);
 	 }

 	 /* printf("thread/0x%08lx\n",pthread_self()); */

 	 if((strlen(buffer)==0) || (strcmp(buffer,"\n")==0) || (strcmp(buffer,"/")==0) || (strcmp(buffer,"_")==0)){
  	  if((out_buf=malloc(STR_CHARS)) == 0){
  	   sysfatal("can't malloc out_buf in GetAndSend: %r");
  	  }
  	  sprintf(out_buf,"Wrong string format\n");
  	  cmd=strdup("\0");
  	  lrms=strdup("\0");
  	  jobid=strdup("\0");
  	  fulljobid=strdup("\0");
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
  	  cmd=strdup(tbuf[0]);
  	 }else{
  	  if((cmd=malloc(STR_CHARS)) == 0){
  	   sysfatal("can't malloc cmd in GetAndSend: %r");
  	  }
  	  cmd=strdup("\0");
 	 }
  	 if(tbuf[1]){
  	  fulljobid=strdup(tbuf[1]);
  	 }else{
  	  if((fulljobid=malloc(STR_CHARS)) == 0){
  	   sysfatal("can't malloc fulljobid in GetAndSend: %r");
  	  }
  	  fulljobid=strdup("\0");
  	 }

  	 for(ii=0;ii<maxtok;ii++){
  	  free(tbuf[ii]);
  	 }
  	 free(tbuf);
	 
  	 if((tbuf=malloc(10 * sizeof *tbuf)) == 0){
  	   sysfatal("can't malloc tbuf: %r");
  	 }
  	 maxtok=strtoken(fulljobid,'_',tbuf);
	 
	 if(tbuf[0]){
  	  lrms=strdup(tbuf[0]);
  	 }else{
  	  if((lrms=malloc(STR_CHARS)) == 0){
  	   sysfatal("can't malloc lrms in GetAndSend: %r");
  	  }
  	  lrms=strdup("\0");
  	 }

	 if(tbuf[1]){
  	  jobid=strdup(tbuf[1]);
  	 }else{
  	  if((jobid=malloc(STR_CHARS)) == 0){
  	   sysfatal("can't malloc jobid in GetAndSend: %r");
  	  }
  	  jobid=strdup("\0");
  	 }

	 
  	 for(ii=0;ii<maxtok;ii++){
  	  free(tbuf[ii]);
  	 }
  	 free(tbuf);

  	 if((strcmp(cmd,"REG")==0)){
	  WriteDN(jobid,lrms);
 	  sprintf(out_buf,"\n");
 	  goto close;
 	 }else if((strcmp(cmd,"GET")==0)){
	  if(ReadDN(jobid,lrms)!=0){
 	   sprintf(out_buf,"\n");
	   goto close;
	  }
	  
 	  dgas_out=ParseDGASFile(jobid,lrms);
 	  sprintf(out_buf,"%s\n", dgas_out);
	  free(dgas_out);
 	  goto close;
 	 }else{
 	  sprintf(out_buf,"\n");
 	  goto close;
 	 }
	
        } /*close GSS if*/
	
close:	
 	Writeline(conn_s, out_buf, strlen(out_buf));
	if(debug){
	 fprintf(stderr, "Sent:%s",out_buf);
	}

	free(out_buf);
	free(buffer);
	free(cmd);
	free(fulljobid);
	free(jobid);
	free(lrms);
	
        gss_release_cred(&major_status, &credential_handle);
        gss_delete_sec_context(&minor_status, &context_handle, GSS_C_NO_BUFFER);
	/*  Close the connected socket  */

	if ( close(conn_s) < 0 ) {
	    fprintf(stderr, "%s: Error calling close()\n",progname);
	    exit(EXIT_FAILURE);
	}
	
    } /* closes while */
    return(0); 
}

char *ParseDGASFile(char *jobid, char *lrms){

 char *command_string;
 char *dgasfile;
 FILE *echo_output;
 char *echo_out;
 int len;
 
 if((dgasfile=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc dgasfile in ParseDGASFile: %r");
 }
 dgasfile[0]='\0';

 if((command_string=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc command_string in ParseDGASFile: %r");
 }
 command_string[0]='\0';
 
 if((echo_out=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc echo_out in ParseDGASFile: %r");
 }
 echo_out[0]='\0';
 
 strcat(dgasfile,spooldir); 
 strcat(dgasfile,"/"); 
 strcat(dgasfile,lrms); 
 strcat(dgasfile,"_"); 
 strcat(dgasfile,jobid); 
 
 sprintf(command_string,"cat %s 2>/dev/null",dgasfile);
 echo_output = popen(command_string,"r");
 if (echo_output != NULL){
  len = fread(echo_out, sizeof(char), MAX_CHARS - 1 , echo_output);
  if (len>0){
   echo_out[len-1]='\000';
  }else{
   echo_out[0]='\0';
  }
 }else{
  echo_out[0]='\0';
 }
 pclose(echo_output);
 
 free(dgasfile);
 free(command_string);
 
 return echo_out;
}

int WriteDN(char *jobid, char *lrms){

 char *client_dn;
 char *command_string;
 char *dnfile;
 FILE *echo_output;
 char *echo_out;
 int len;
 
 client_dn=verify_dn(context_handle);
 
 if((dnfile=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc dnfile in WriteDN: %r");
 }
 dnfile[0]='\0';
 
 if((echo_out=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc echo_out in WriteDN: %r");
 }
 echo_out[0]='\0';

 if((command_string=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc command_string in WriteDN: %r");
 }
 command_string[0]='\0';
 
 strcat(dnfile,spooldir); 
 strcat(dnfile,"/"); 
 strcat(dnfile,lrms); 
 strcat(dnfile,"_"); 
 strcat(dnfile,jobid); 
 strcat(dnfile,".dnfile"); 
 
 sprintf(command_string,"if [ ! -f  %s ]; then echo \"%s\" > %s; fi",dnfile,client_dn,dnfile);
 echo_output = popen(command_string,"r");
 if (echo_output != NULL){
  len = fread(echo_out, sizeof(char), sizeof(echo_out) - 1 , echo_output);
  if (len>0){
   echo_out[len-1]='\000';
  }
 }
 pclose(echo_output);
 
 free(dnfile);
 free(echo_out);
 free(command_string);
   
 return 0;
 
}

int ReadDN(char *jobid, char *lrms){

 char *client_dn;
 char *command_string;
 char *dnfile;
 FILE *echo_output;
 char *echo_out;
 int len,retcode;

 client_dn=verify_dn(context_handle);

 if((dnfile=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc dnfile in ReadDN: %r");
 }
 dnfile[0]='\0';
 
 if((command_string=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc command_string in ReadDN: %r");
 }
 command_string[0]='\0';

 if((echo_out=malloc(MAX_CHARS)) == 0){
  sysfatal("can't malloc echo_out in ReadDN: %r");
 }
 echo_out[0]='\0';

 strcat(dnfile,spooldir); 
 strcat(dnfile,"/");  
 strcat(dnfile,lrms); 
 strcat(dnfile,"_"); 
 strcat(dnfile,jobid); 
 strcat(dnfile,".dnfile"); 

 sprintf(command_string,"cat %s 2>/dev/null",dnfile);
 echo_output = popen(command_string,"r");
 if (echo_output != NULL){
  len = fread(echo_out, sizeof(char), MAX_CHARS - 1 , echo_output);
  if (len>0){
   echo_out[len-1]='\000';
  }else{
   echo_out[0]='\0';
  }
 }else{
  echo_out[0]='\0';
  retcode=-1;
 }
 
 pclose(echo_output);
 if(strcmp(echo_out,client_dn)==0){
  retcode=0;
 }else{
  retcode=-1;
 }

 free(dnfile);
 free(command_string);
 free(echo_out);
  
 return retcode;
}
char *verify_dn(gss_ctx_id_t context_handle)
{
        OM_uint32       major_status = 0;
        OM_uint32       minor_status = 0;
        OM_uint32       ret_flags = 0;
        gss_name_t      target_name = GSS_C_NO_NAME;
        gss_name_t      src_name = GSS_C_NO_NAME;
        gss_buffer_desc name_buffer  = GSS_C_EMPTY_BUFFER;

        char    *target_name_str = NULL;
        char    *src_name_str = NULL;

        major_status = gss_inquire_context(
                &minor_status, /* minor_status */
                context_handle, /* context_handle */
                &src_name,  /* The client principal name */
                &target_name, /* The server principal name */
                NULL, /* don't need user_to_user */
                NULL, /* don't need user_to_user */
                NULL, /* don't need user_to_user */
                NULL, /* don't need user_to_user */
                NULL  /* don't need user_to_user */
                );
        CHECK_GLOBUS_CALL("GSS context inquire failure ", 1, major_status);

       /* Get the client principal name */
        major_status = gss_display_name(
                &minor_status,
                src_name,
                &name_buffer,
                NULL);
        CHECK_GLOBUS_CALL("GSS display_name failure ", 1, major_status);
        if ((src_name_str = (char *)malloc((name_buffer.length + 1) * sizeof(char))) == NULL)
        {
                fprintf(stderr, "verify_context(): Out of memory\n");
                return(1);
        }
        memcpy(src_name_str, name_buffer.value, name_buffer.length);
        src_name_str[name_buffer.length] = '\0';
        major_status = gss_release_name(&minor_status, &target_name);
        major_status = gss_release_buffer(&minor_status, &name_buffer);

        /* Strip trailing "/CN=proxy" */
        while (strcmp(src_name_str + strlen(src_name_str) - 9, "/CN=proxy") == 0)
                src_name_str[strlen(src_name_str) - 9] = '\0';

        return src_name_str;
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

}

void print_usage(){

   fprintf(stderr,"Usage:\n");
   fprintf(stderr,"%s [-p] <remote_port [%d]> -s <DGAS_spooldir [%s]>\n",progname, DEFAULT_PORT, spooldir);
   fprintf(stderr,"Use -d to enable debugging.\n");        
   exit(EXIT_SUCCESS);

}

int ParseCmdLine(int argc, char *argv[], char **szPort, char **szSpoolDir) {
    
    int n = 1;
     
    *szPort=NULL;
    if(argc==2 && (!strncmp(argv[n], "-d", 2) || !strncmp(argv[n], "-D", 2))){
      debug=1;
      return 0;
    }else if(argc==2 && !(!strncmp(argv[n], "-h", 2) || !strncmp(argv[n], "-H", 2))){
     *szPort= argv[n];
     return 0;
    }
    

    while ( n < argc ) {
        if ( !strncmp(argv[n], "-p", 2) ) {
            *szPort= argv[++n];
        }
        else if ( !strncmp(argv[n], "-s", 2) ) {
            *szSpoolDir = argv[++n];
        }
        else if ( !strncmp(argv[n], "-d", 2) ) {
	    debug=1;
        }
        else if ( !strncmp(argv[n], "-D", 2) ) {
	    dmn=1;
        }
        else if ( !strncmp(argv[n], "-h", 2) ) {
            print_usage();
        }
        ++n;
    }
    
    return 0;
}

int strtoken(const char *s, char delim, char **token)
{
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

/* the reset is error processing stuff */

void
eprint(int err, char *fmt, va_list args)
{
    extern int errno;

    fprintf(stderr, "%s: ", argv0);
    if(fmt)
        vfprintf(stderr, fmt, args);
    if(err)
        fprintf(stderr, "%s", strerror(errno));
    fputs("\n", stderr);
    errno = 0;
}

char *
chopfmt(char *fmt)
{
    static char errstr[ERRMAX];
    char *p;

    errstr[0] = '\0';
    if((p=strstr(fmt, "%r")) != 0)
        fmt = strncat(errstr, fmt, p-fmt);
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
    exit(1);
}

