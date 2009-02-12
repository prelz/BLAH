#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <syslog.h>
#include <mcheck.h>
#include <pthread.h>
#include <wordexp.h>
#include <signal.h>

#include "blahpd.h"
#include "config.h"

#define CONFIG_FILE_PARSER "blparser.conf"
#define PID_DIR "/var/run"
#define MAXPARSERNUM 30
#define PARSER_COMMAND_LINE_SIZE 500
#define PID_FILENAME_SIZE 200

/* Global variables */
struct blah_managed_child {
	char *exefile;
	char *pidfile;
};
static struct blah_managed_child *parser_pbs=NULL;
static struct blah_managed_child *parser_lsf=NULL;

int usepbs=0;
int uselsf=0;
int parsernumpbs=0;
int parsernumlsf=0;

static pthread_mutex_t bfork_lock  = PTHREAD_MUTEX_INITIALIZER;

config_handle *blah_config_handle;
config_entry *ret;

char *config_file=NULL;

/* Functions */

/* Check on good health of our managed children
 **/
void check_on_children_args(const struct blah_managed_child *children, const int count)
{
	FILE *pid;
        FILE *fpid;
	pid_t ch_pid;
	int junk;
	int i,j;
	int try_to_restart;
	int fret;
	static time_t lastfork=0;
	time_t new_lastfork;
	static time_t calldiff=0;
	const time_t default_calldiff = 5;
	config_entry *ccld;
	time_t now;

        wordexp_t args;
	
	pthread_mutex_lock(&bfork_lock);
	if (calldiff <= 0)
	{
		ccld = config_get("blah_children_restart_interval",blah_config_handle);
		if (ccld != NULL) calldiff = atoi(ccld->value);
		if (calldiff <= 0) calldiff = default_calldiff;
	}

	time(&now);
	new_lastfork = lastfork;

	for (i=0; i<count; i++)
	{
		try_to_restart = 0;
		if ((pid = fopen(children[i].pidfile, "r")) == NULL)
		{
			if (errno != ENOENT) continue;
			else try_to_restart = 1;
		} else {
			if (fscanf(pid,"%d",&ch_pid) < 1) 
			{
				fclose(pid);
				continue;
			}
			if (kill(ch_pid, 0) < 0)
			{
				/* The child process disappeared. */
				if (errno == ESRCH) try_to_restart = 1;
			}
			fclose(pid);
		}
		if (try_to_restart)
		{
			/* Don't attempt to restart too often. */
			if ((now - lastfork) < calldiff) 
			{
				fprintf(stderr,"Restarting %s too frequently.\n",
					children[i].exefile);
				fprintf(stderr,"Last restart %d seconds ago (<%d).\n",
					(int)(now-lastfork), calldiff);
				continue;
			}
			fret = fork();
			if (fret == 0)
			{
				if(j = wordexp(children[i].exefile, &args, 0))
				{
					fprintf(stderr,"wordexp: unable to parse the command line \"%s\" (error %d)\n", children[i].exefile, j);
                			return NULL;
        			}
				/* Child process. Exec exe file. */
				if (execv(args.we_wordv[0], args.we_wordv) < 0)
				{
					fprintf(stderr,"Cannot exec %s: %s\n",
						children[i].exefile,
						strerror(errno));
					exit(1);
				}
				/* Free the wordexp'd args */
 				wordfree(&args);

			} else if (fret < 0) {
				fprintf(stderr,"Cannot fork trying to start %s: %s\n",
					children[i].exefile,
					strerror(errno));
			} else {
				if (kill(fret, 0) < 0){
					/* The child process disappeared. */
					if (errno == ESRCH) try_to_restart = 1;
				}else{
        				fpid = fopen(children[i].pidfile, "w");
        				if ( !fpid ) { perror(children[i].pidfile); return 1; }
					if (fprintf(fpid, "%d", fret) <= 0) { perror(children[i].pidfile); return 1; }
        				if (fclose(fpid) != 0) { perror(children[i].pidfile); return 1; }
				}
			}
			new_lastfork = now;
		}
	}

	/* Reap dead children. Yuck.*/
	while (waitpid(-1, &junk, WNOHANG) > 0) /* Empty loop */;

	lastfork = new_lastfork;

	pthread_mutex_unlock(&bfork_lock);
}

void sigterm(){
			
	int i;
	FILE *pid;
	pid_t ch_pid;

	if(usepbs){
		for(i=0;i<parsernumpbs;i++){
			if ((pid = fopen(parser_pbs[i].pidfile, "r")) != NULL){
				if (fscanf(pid,"%d",&ch_pid) < 1){
					fclose(pid);
					continue;
				}
			}
			fclose(pid);
			merciful_kill_noglexec(ch_pid);	
		}
	}
	if(uselsf){
		for(i=0;i<parsernumlsf;i++){
			if ((pid = fopen(parser_lsf[i].pidfile, "r")) != NULL){
				if (fscanf(pid,"%d",&ch_pid) < 1){
					fclose(pid);
					continue;
				}
			}
			fclose(pid);
			merciful_kill_noglexec(ch_pid);	
		}
	}
	exit(0);

}

char *
make_message(const char *fmt, ...)
{
	int n;
	char *result = NULL;
	va_list ap;

	va_start(ap, fmt);
	n = vsnprintf(NULL, 0, fmt, ap) + 1;

	result = (char *) malloc (n);
	if (result)
		vsnprintf(result, n, fmt, ap);
	va_end(ap);

	return(result);
}

void
daemonize()
{

        int pid;

        pid = fork();

        if (pid < 0){
		fprintf(stderr, "Cannot fork in daemonize\n");
                exit(EXIT_FAILURE);
        }else if (pid >0){
                exit(EXIT_SUCCESS);
        }

        setsid();

        pid = fork();

        if (pid < 0){
		fprintf(stderr, "Cannot fork in daemonize\n");
                exit(EXIT_FAILURE);
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
merciful_kill_noglexec(pid_t pid)
{
	int graceful_timeout = 1; /* Default value - overridden by config */
	int status=0;
	config_entry *config_timeout;
	int tmp_timeout;
	int tsl;
	int kill_status;

	if (blah_config_handle != NULL && 
	    (config_timeout=config_get("blah_graceful_kill_timeout",blah_config_handle)) != NULL)
	{
		tmp_timeout = atoi(config_timeout->value);
		if (tmp_timeout > 0) graceful_timeout = tmp_timeout;
	}

	/* verify that child is dead */
	for(tsl = 0; (waitpid(pid, &status, WNOHANG) == 0) &&
	              tsl < graceful_timeout; tsl++)
	{
		/* still alive, allow a few seconds 
		   than use brute force */
		if (tsl > (graceful_timeout/2)) 
		{
			/* Signal forked process group */
			kill(pid, SIGTERM);
		}
	}

	if (tsl >= graceful_timeout && (waitpid(pid, &status, WNOHANG) == 0))
	{
		kill_status = kill(pid, SIGKILL);

		if (kill_status == 0)
		{
			waitpid(pid, &status, 0);
		}
	}

	return(status);
}

int 
main(int argc, char *argv[])
{

	char *blah_location=NULL;
        char *parser_names[3] = {"BLParserPBS", "BLParserLSF", NULL};
	
	char *useparserpbs=NULL;
	char *debuglevelpbs=NULL;
	char *debuglogfilepbstmp=NULL;
	char *debuglogfilepbs=NULL;
	char *spooldirpbs=NULL;
	
	char *useparserlsf=NULL;
	char *debuglevellsf=NULL;
	char *debuglogfilelsftmp=NULL;
	char *debuglogfilelsf=NULL;
	char *binpathlsf=NULL;
	char *confpathlsf=NULL;
	
	char *portpbskey=NULL;
	char *creamportpbskey=NULL;
	char *portpbs=NULL;
	char *creamportpbs=NULL;
	
	char *portlsfkey=NULL;
	char *creamportlsfkey=NULL;
	char *portlsf=NULL;
	char *creamportlsf=NULL;
	
	int i,j;
	
		
/* Read config common part */

	if ((blah_location = getenv("GLITE_LOCATION")) == NULL)
	{
		blah_location = getenv("BLAHPD_LOCATION");
		if (blah_location == NULL) blah_location = DEFAULT_GLITE_LOCATION;
	}
	
	config_file = (char *)malloc(strlen(CONFIG_FILE_PARSER)+strlen(blah_location)+6);
	if (config_file == NULL) return NULL;
	sprintf(config_file,"%s/etc/%s",blah_location,CONFIG_FILE_PARSER);

        blah_config_handle = config_read(config_file);
        if (blah_config_handle == NULL)
        {
                fprintf(stderr,"Error reading config: ");
                perror("");
                return -1;
        }

        ret = config_get("GLITE_CE_BLPARSERPBS_DAEMON",blah_config_handle);
        if (ret != NULL){
                useparserpbs=strdup(ret->value);
                if(useparserpbs && strstr(useparserpbs,"yes")){
			usepbs=1;
		}
		if(useparserpbs) free(useparserpbs);
        }
	
        ret = config_get("GLITE_CE_BLPARSERLSF_DAEMON",blah_config_handle);
        if (ret != NULL){
                useparserlsf=strdup(ret->value);
                if(useparserlsf && strstr(useparserlsf,"yes")){
			uselsf=1;
		}
		if(useparserlsf) free(useparserlsf);
        }
	
	parser_pbs=malloc(MAXPARSERNUM*sizeof(struct blah_managed_child));
	parser_lsf=malloc(MAXPARSERNUM*sizeof(struct blah_managed_child));
	
	if(parser_pbs == NULL || parser_lsf == NULL){
		fprintf(stderr, "Out of memory\n");
		exit(MALLOC_ERROR);
	}
	
	/* PBS part */
	
	if(usepbs){

        	ret = config_get("GLITE_CE_BLPARSERPBS_DEBUGLEVEL",blah_config_handle);
        	if (ret != NULL){
			debuglevelpbs = make_message("-d %s",ret->value);
        	}

        	ret = config_get("GLITE_CE_BLPARSERPBS_DEBUGLOGFILE",blah_config_handle);
        	if (ret != NULL){
			debuglogfilepbstmp = make_message("-l %s",ret->value);
        	}

        	ret = config_get("GLITE_CE_BLPARSERPBS_SPOOLDIR",blah_config_handle);
        	if (ret != NULL){
			spooldirpbs = make_message("-s %s",ret->value);
        	}

        	ret = config_get("GLITE_CE_BLPARSERPBS_NUM",blah_config_handle);
        	if (ret != NULL){
        	        parsernumpbs=atoi(ret->value);
        	}else{
			parsernumpbs=1;
		}
	
		for(i=0;i<parsernumpbs;i++){
			portpbskey = make_message("GLITE_CE_BLPARSERPBS_PORT%d",i+1);
			creamportpbskey = make_message("GLITE_CE_BLPARSERPBS_CREAMPORT%d",i+1);
			if(portpbskey == NULL || creamportpbskey == NULL){
				fprintf(stderr, "Out of memory\n");
				exit(MALLOC_ERROR);
			}
			ret = config_get(portpbskey,blah_config_handle);
			if (ret != NULL){
				portpbs = make_message("-p %s",ret->value);
			}
			ret = config_get(creamportpbskey,blah_config_handle);
			if (ret != NULL){
				creamportpbs = make_message("-m %s",ret->value);
			}
			if(parsernumpbs>1){
				debuglogfilepbs=make_message("%s-%d",debuglogfilepbstmp,i+1);
			}else{
				debuglogfilepbs=strdup(debuglogfilepbstmp);
			}
			parser_pbs[i].exefile = make_message("%s/bin/%s %s %s %s %s %s",blah_location,parser_names[0],debuglevelpbs,debuglogfilepbs,spooldirpbs,portpbs,creamportpbs);
			parser_pbs[i].pidfile = make_message("%s/%s%d.pid",PID_DIR,parser_names[0],i+1);
			
			/* Do the shell expansion */	
			free(portpbskey);
			free(creamportpbskey);
			if(portpbs) free(portpbs);
			if(creamportpbs) free(creamportpbs);
		}
		if(debuglevelpbs) free(debuglevelpbs);
		if(debuglogfilepbstmp) free(debuglogfilepbstmp);
		if(debuglogfilepbs) free(debuglogfilepbs);
		if(spooldirpbs) free(spooldirpbs);
	}
	
	/* LSF part */
	
	if(uselsf){

        	ret = config_get("GLITE_CE_BLPARSERLSF_DEBUGLEVEL",blah_config_handle);
        	if (ret != NULL){
			debuglevellsf = make_message("-d %s",ret->value);
        	}

        	ret = config_get("GLITE_CE_BLPARSERLSF_DEBUGLOGFILE",blah_config_handle);
        	if (ret != NULL){
			debuglogfilelsftmp = make_message("-l %s",ret->value);
        	}

        	ret = config_get("GLITE_CE_BLPARSERLSF_BINPATH",blah_config_handle);
        	if (ret != NULL){
			binpathlsf = make_message("-b %s",ret->value);
        	}

        	ret = config_get("GLITE_CE_BLPARSERLSF_CONFPATH",blah_config_handle);
        	if (ret != NULL){
			confpathlsf = make_message("-c %s",ret->value);
        	}

        	ret = config_get("GLITE_CE_BLPARSERLSF_NUM",blah_config_handle);
        	if (ret != NULL){
        	        parsernumlsf=atoi(ret->value);
        	}else{
			parsernumlsf=1;
		}

		for(i=0;i<parsernumlsf;i++){
			portlsfkey = make_message("GLITE_CE_BLPARSERLSF_PORT%d",i+1);
			creamportlsfkey = make_message("GLITE_CE_BLPARSERLSF_CREAMPORT%d",i+1);
			if(portlsfkey == NULL || creamportlsfkey == NULL){
				fprintf(stderr, "Out of memory\n");
				exit(MALLOC_ERROR);
			}
			ret = config_get(portlsfkey,blah_config_handle);
			if (ret != NULL){
				portlsf = make_message("-p %s",ret->value);
			}
			ret = config_get(creamportlsfkey,blah_config_handle);
			if (ret != NULL){
				creamportlsf = make_message("-m %s",ret->value);
			}
			if(parsernumpbs>1){
				debuglogfilelsf=make_message("%s-%d",debuglogfilelsftmp,i+1);
			}else{
				debuglogfilelsf=strdup(debuglogfilelsftmp);
			}
			parser_lsf[i].exefile = make_message("%s/bin/%s %s %s %s %s %s %s",blah_location,parser_names[1],debuglevellsf,debuglogfilelsf,binpathlsf,confpathlsf,portlsf,creamportlsf);
			parser_lsf[i].pidfile = make_message("%s/%s%d.pid",PID_DIR,parser_names[1],i+1);
			
			/* Do the shell expansion */	
			if(portlsfkey) free(portlsfkey);
			if(creamportlsfkey) free(creamportlsfkey);
			if(portlsf) free(portlsf);
			if(creamportlsf) free(creamportlsf);
		}
		if(debuglevellsf) free(debuglevellsf);
		if(debuglogfilelsftmp) free(debuglogfilelsftmp);
		if(debuglogfilelsf) free(debuglogfilelsf);
		if(binpathlsf) free(binpathlsf);
		if(confpathlsf) free(confpathlsf);
	}
	
	/* signal handler */
	
	signal(SIGTERM,sigterm);
	
	daemonize();
		
	while(1){
	
		
		if (usepbs && parsernumpbs>0) check_on_children_args(parser_pbs, parsernumpbs);
		if (uselsf && parsernumlsf>0) check_on_children_args(parser_lsf, parsernumlsf);
		sleep(2);
	}
		
	exit(EXIT_SUCCESS);
}
