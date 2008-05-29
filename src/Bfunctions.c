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

char *get_line(FILE * f)
{
    size_t size = 0;
    size_t len  = 0;
    size_t last = 0;
    char * buf  = NULL;

    do {
        size += BUFSIZ;
        buf = realloc(buf,size);           
	fgets(buf+last,size-last,f);
        len = strlen(buf);
        last = len - 1;
    } while (!feof(f) && buf[last]!='\n');
    return buf;
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
		tmp = strndup(s, STR_CHARS);
		return tmp;
	}
        
	if(!s || !strlen(s)){
		tmp = strndup(s, STR_CHARS);
		return tmp;
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
        }else if(strcmp(f,"A")==0){
                strptime(str,"%m/%d/%Y %T",tm);
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
        }else if(strcmp(f,"V")==0){

        /* If do not have the year in the date we compare day and month and set the year */

                if((strtmp=calloc(STR_CHARS,1)) == 0){
                        sysfatal("can't malloc strtmp in str2epoch: %r");
                }

                sprintf(strtmp,"%s 2000",str);
                strptime(strtmp,"%b %d %H:%M %Y",tm);
                
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
 
	tm->tm_isdst=-1;
	idate=mktime(tm);
 
	free(tm);
 
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

int
bupdater_push_active_job(bupdater_active_jobs *bact, const char *job_id)
{
  char **new_jobs;

  new_jobs = (char **) realloc(bact->jobs, (bact->njobs+1) * sizeof(char *));
  if (new_jobs == NULL) return BUPDATER_ACTIVE_JOBS_FAILURE;

  bact->jobs = new_jobs;
  bact->jobs[bact->njobs] = strdup(job_id);
  if (bact->jobs[bact->njobs] == NULL) return BUPDATER_ACTIVE_JOBS_FAILURE;
  bact->njobs++;

  bact->is_sorted = 0;

  return BUPDATER_ACTIVE_JOBS_SUCCESS;
}

void
bupdater_sort_active_jobs(bupdater_active_jobs *bact, int left, int right)
{
  int psize;
  int mother = 0;
  char *max, *median, *min, *pick, *swap;
  int i,k;

  if (left == 0 && right == bact->njobs-1) mother = 1;
  if (mother != 0)
   {
    if (bact->is_sorted != 0) return;
    if (bact->njobs < 2) return;
    srand(time(0));
   }

  /* Singly-recursive quicksort of job entries  */

  while (left < right)
   {
    psize = right - left + 1;

    /* Choose a partition value with the "median-of-three" method. */
    max = min = bact->jobs[left + rand()%psize];

    pick = bact->jobs[left + rand()%psize];
    if (strcmp(pick, max) > 0) max = pick;
    else if (strcmp (pick, min) < 0) min = pick;

    pick = bact->jobs[left + rand()%psize];
    if (strcmp(pick, max) > 0) median = max;
    else if (strcmp(pick, min) < 0) median = min;
    else median = pick;

    for (i = left, k = right; ; i++,k--)
     {
      while (strcmp(bact->jobs[i], median) < 0) i++;
      while (strcmp(bact->jobs[k], median) > 0) k--;

      /* Now stop if indexes crossed. This way we are sure that k is the
      /* last element of the left partition. */
      if (i>=k) break;

      /* We found a pair that's out of order. Let's swap them. */
      swap = bact->jobs[i];
      bact->jobs[i] = bact->jobs[k];
      bact->jobs[k] = swap;
     }

    /* Operate on the left and right sub-partitions. */
    bupdater_sort_active_jobs(bact,left,k);

    /* Do the right partition on the next while loop iteration */
    left = k+1;
   }

  if (mother != 0)
   {
    bact->is_sorted = 1;
   }
  return;
}

int
bupdater_lookup_active_jobs(bupdater_active_jobs *bact, 
                            const char *job_id)
{
  int left, right, cur, cmp;
  if (bact->is_sorted == 0) bupdater_sort_active_jobs(bact,0,bact->njobs-1);

  /* Binary search of needed entry */
  left = 0; right = bact->njobs-1;

  while (right >= left)
   {
    cur = (right + left) /2;
    cmp = strcmp(bact->jobs[cur],job_id);
    if (cmp == 0)
     {
      return BUPDATER_ACTIVE_JOBS_SUCCESS;
     }
    else if (cmp < 0)
     {
      left = cur+1;
     }
    else
     {
      right = cur-1;
     }
   }

  return BUPDATER_ACTIVE_JOBS_FAILURE;
}

void
bupdater_free_active_jobs(bupdater_active_jobs *bact)
{
  int i;
  if (bact->jobs == NULL) return;

  for (i=0; i<bact->njobs; i++)
    if (bact->jobs[i] != NULL) free(bact->jobs[i]);

  free(bact->jobs); 
  bact->jobs = NULL;
  bact->njobs = 0;
}

