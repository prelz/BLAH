#define _GNU_SOURCE
#define _XOPEN_SOURCE
#include <unistd.h>    
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h> 
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
#define DEFAULT_PORT       33333 
#define MAX_CHARS          100000
#define STR_CHARS          3000
#define NUM_CHARS          300
#define RDXHASHSIZE        20000
#define CRMHASHSIZE        60000
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
char *GetLogDir(int largc, char *largv[]);
char *GetLogList(char *logdate);
void CreamConnection(int c_sock);
int NotifyFromDate(char *in_buf);
int NotifyCream(int jobid, char *newstatus, char *blahjobid, char *wn, char *reason, char *timestamp, int flag);
int UpdatePtr(int jid, char *rx, int has_bl);
int GetRdxId(int cnt);
int GetBlahNameId(char *blahstr);
int strtoken(const char *s, char delim, char **token);
char *strdel(char *s, const char *delete);
char *epoch2str(char *epoch);
char *iepoch2str(int epoch);
int str2epoch(char *str, char *f);
void daemonize();
void eprint(int err, char *fmt, va_list args);
char *chopfmt(char *fmt);
void syserror(char *fmt, ...);
void sysfatal(char *fmt, ...);

/* Variables initialization */

int rptr[RDXHASHSIZE];

int reccnt[RDXHASHSIZE];

char *j2js[RDXHASHSIZE];
char *j2wn[RDXHASHSIZE];
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

char *progname="BLParserLSF";

int port;
int creamport;
int usecream=0;

int debug=0;
int dmn=0;

FILE *debuglogfile;
char *debuglogname="/opt/glite/var/log/BLParserLSF.log";;

struct sockaddr_in cservaddr;

int  list_c;
int  conn_c=-1;
int  c_sock;

/* 
to know if cream is connected:
0 - not connected
1 - connected
*/
int creamisconn=0;

/* confpath and binpath default */
char *confpath="/etc";
char *binpath="/usr/local/lsf/bin";

char *lsbevents="lsb.events";
char *ldir;
char *eventsfile;

char *blank=" ";

char * rex_queued   = "\"JOB_NEW\"";
char * rex_running  = "\"JOB_START\"";
char * rex_status   = "\"JOB_STATUS\"";
char * rex_signal   = "\"JOB_SIGNAL\"";
