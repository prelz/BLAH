#define _GNU_SOURCE
#define _XOPEN_SOURCE
#include <unistd.h>    
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h> 
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/select.h>
#include <sys/poll.h>
#include <popt.h>
#include <dirent.h>

#define LISTENQ            1024
#define DEFAULT_PORT       33332
#define MAX_CHARS          100000
#define STR_CHARS          3000
#define NUM_CHARS          300
#define RDXHASHSIZE        60000
#define CRMHASHSIZE        360000
#define NUMTHRDS           3
#define ERRMAX             80
#define TBUFSIZE           400 
#define WRETRIES           10

#ifndef VERSION
#define VERSION            "1.8.0"
#endif


/*  Function declarations  */

ssize_t Readline(int fd, void *vptr, size_t maxlen);
ssize_t Writeline(int fc, const void *vptr, size_t maxlen);
void *mytail (void *infile);    
void follow(char *infile, char *line);
long tail(FILE *fp, char *line, long old_off);
int InfoAdd(int id, char *value, const char * flag);
int AddToStruct(char *o_buffer, int flag);
char *GetAllEvents(char *file);
void *LookupAndSend (int m_sock); 
int GetEventsInOldLogs(char *logdate);
char *GetLogList(char *logdate);
void CreamConnection(int c_sock);
int NotifyFromDate(char *in_buf);
int NotifyCream(int jobid, char *newstatus, char *blahjobid, char *wn, char *reason, char *timestamp, int flag);
int UpdatePtr(int jid, char *fulljobid, int is_que, int has_bl);
int GetRdxId(int cnt);
int GetBlahNameId(char *blahstr);
int strtoken(const char *s, char delim, char **token);
char *convdate(char *date);
char *strdel(char *s, const char *delete);
char *iepoch2str(int epoch, char *f);
int str2epoch(char *str, char *f);
void daemonize();
void sighup();
void eprint(int err, char *fmt, va_list args);
char *chopfmt(char *fmt);
void syserror(char *fmt, ...);
void sysfatal(char *fmt, ...);

/* Variables initialization */

int rptr[RDXHASHSIZE];
char *rfullptr[RDXHASHSIZE];

int reccnt[RDXHASHSIZE];

char *j2js[RDXHASHSIZE];
char *j2ec[RDXHASHSIZE];
char *j2st[RDXHASHSIZE];
char *j2rt[RDXHASHSIZE];
char *j2ct[RDXHASHSIZE];

char *j2bl[RDXHASHSIZE];

int   nti[CRMHASHSIZE];
char *ntf[CRMHASHSIZE];

char *argv0;

char *blahjob_string="blahjob_";
char *bl_string="bl_";
char *cream_string="cream_";

int jcount=0;
int ptrcnt=1;

int recycled=0;
int cream_recycled=0;

pthread_mutex_t cr_write_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t writeline_mutex = PTHREAD_MUTEX_INITIALIZER;

char *progname="BLParserPBS";

char *ldir;

int port;
int creamport;
int usecream=0;

int debug=0;
int dmn=0;

FILE *debuglogfile;
char *debuglogname="/opt/glite/var/log/BLParserPBS.log";

int  conn_c=-1;
int  c_sock;

/* 
to know if cream is connected:
0 - not connected
1 - connected
*/
int creamisconn=0;

/* spooldir default */
char *spooldir="/usr/spool/PBS";

char cnow[30];

char *blank=" ";

char * rex_queued    = "Job Queued ";
char * rex_running   = "Job Run ";
char * rex_deleted   = "Job deleted ";
char * rex_finished  = "Exit_status=";
char * rex_unable    = "unable to run job";
char * rex_hold      = "Holds";
char * rex_uhold     = "Holds u set";
char * rex_ohold     = "Holds o set";
char * rex_shold     = "Holds s set";
char * rex_uresume   = "Holds u released";
char * rex_oresume   = "Holds o released";
char * rex_sresume   = "Holds s released";
char * rex_dequeue   = "dequeuing from";
char * rex_staterun  = "state RUNNING";
