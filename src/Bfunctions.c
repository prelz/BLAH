#include "Bfunctions.h"

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
	
	if((tm=calloc(STR_CHARS,1)) == 0){
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
 
        strftime(dateout,NUM_CHARS,"%Y-%m-%d %T",tm);
	free(tm);
	free(lepoch);
 
	return dateout;
 
}

int
str2epoch(char *str, char * f)
{
  
	char *dateout;
	char *strtmp;
	int idate;
	time_t now;

	struct tm *tm;
        struct tm *tmnow;
	
	int mdlog,mdnow;
	
	if((tm=calloc(NUM_CHARS,1)) == 0){
		sysfatal("can't malloc tm in str2epoch: %r");
	}
	if(strcmp(f,"S")==0){
		strptime(str,"%Y-%m-%d %T",tm);
	}else if(strcmp(f,"L")==0){
		strptime(str,"%a %b %d %T %Y",tm);
	}else if(strcmp(f,"W")==0){
		
	/* If do not have the year in the date we compare day and month and set the year */
		
		if((strtmp=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc strtmp in str2epoch: %r");
		}
	
		sprintf(strtmp,"%s 2000",str);
                strptime(strtmp,"%a %b %d %T %Y",tm);
		
		now=time(0);
		tmnow=localtime(&now);
		
		mdlog=(tm->tm_mon)*100+tm->tm_mday;
		mdnow=(tmnow->tm_mon)*100+tmnow->tm_mday;
		if(mdlog > mdnow){
			tm->tm_year=tmnow->tm_year-1;
		}else{
			tm->tm_year=tmnow->tm_year;
		}
		
	        free(strtmp);
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

int
writepid(char * pidfile)
{
	FILE *fpid;
	
	fpid = fopen(pidfile, "w");
	if ( !fpid ) { perror(pidfile); return 1; }
	if (fprintf(fpid, "%d", getpid()) <= 0) { perror(pidfile); return 1; }
	if (fclose(fpid) != 0) { perror(pidfile); return 1; }
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

