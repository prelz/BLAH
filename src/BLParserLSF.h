#include <unistd.h>    
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h> 
#include <arpa/inet.h> 
#include <unistd.h>    
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/select.h>
#include <sys/poll.h>

#define LISTENQ            1024
#define DEFAULT_PORT       33333 
#define MAX_LINES          100000
#define MAX_CHARS          100000
#define STR_CHARS          3000
#define NUM_CHARS          300
#define HASHSIZE           5000000
#define NUMTHRDS           20
#define CRMTHRDS           8 
#define NLINES             100  /* lines for tail */
#define ERRMAX             80
#define TBUFSIZE           400 
#define WRETRIES           10
#define PURGE_INTERVAL     10
#define PURGE_RETRY        5
#define NUL                '\0'


/*  Function declarations  */

ssize_t Readline(int fd, void *vptr, size_t maxlen);
ssize_t Writeline(int fc, const void *vptr, size_t maxlen);
unsigned hash(char *s);
int AddToStruct(char *o_buffer, int flag);
char *GetAllEvents(char *file);
void *InfoDel ();
void *mytail (void *infile);    
void follow(char *infile, char *line);
long tail(FILE *fp, char *line);
void eprint(int err, char *fmt, va_list args);
char *chopfmt(char *fmt);
void syserror(char *fmt, ...);
void sysfatal(char *fmt, ...);
void *LookupAndSend (int m_sock); 
char *GetLogDir(int largc, char *largv[]);
char *GetLogList(char *logdate);
int GetEventsInOldLogs(char *logdate);
int strtoken(const char *s, char delim, char **token);
int InfoAdd(int id, char *value, const char * flag);
char *InfoGet(int id, const char * flag);
int ParseCmdLine(int argc, char *argv[], char **szPort, char **szBinPath, char **szConfPath, char **szCreamPort);
char *epoch2str(char *epoch);
char *iepoch2str(int epoch);
int str2epoch(char *str, char *f);
void CreamConnection(int c_sock);
int NotifyCream(int jobid, char *newstatus, char *blahjobid, char *wn, char *reason, char *timestamp, int flag);
int NotifyFromDate(char *in_buf);

/* Variables initialization */

char *j2js[HASHSIZE];
char *j2wn[HASHSIZE];
char *j2ec[HASHSIZE];
char *j2st[HASHSIZE];
char *j2rt[HASHSIZE];
char *j2ct[HASHSIZE];

char *bjl[HASHSIZE];
char *j2bl[HASHSIZE];

int   nti[HASHSIZE];
char *ntf[HASHSIZE];

char *argv0;

char *blahjob_string="blahjob_";
char *cream_string="cream_";
//char *cream_string="blahjob_";

int wlock=0;
int cwlock=0;
int rcounter=0;

int jcount=0;

pthread_mutex_t cr_write_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;

char *progname="BLParserLSF";

int port;
int creamport;
int usecream=0;

int debug=0;

struct sockaddr_in cservaddr;

int  list_c;
int  conn_c=-1;
int  c_sock;
char *szCreamPort;

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

char *LastLog=NULL;
char LastLogDate[STR_CHARS]="\0";

char *blank=" ";

char * rex_queued   = "\"JOB_NEW\"";
char * rex_running  = "\"JOB_START\"";
char * rex_status   = "\"JOB_STATUS\"";
char * rex_signal   = "\"JOB_SIGNAL\"";
