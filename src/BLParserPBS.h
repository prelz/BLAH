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

#define LISTENQ            (1024)
#define ECHO_PORT          (2002)
#define MAX_LINES          100000
#define MAX_CHARS          100000
#define STR_CHARS          3000
#define NUM_CHARS          300
#define HASHSIZE           500000 
#define NUMTHRDS           20
#define NLINES             2000  /* lines for tail */
#define ERRMAX             80
#define WRETRIES           10
#define PURGE_INTERVAL     10
#define PURGE_RETRY        5
#define NUL                '\0'
#define DEBUG              0


/*  Function declarations  */

ssize_t Readline(int fd, void *vptr, size_t maxlen);
ssize_t Writeline(int fc, const void *vptr, size_t maxlen);
unsigned hash(char *s);
int AddToStruct(char *o_buffer);
char *GetAllEvents(char *file);
void *InfoDel ();
void *mytail (void *infile);    
void follow(char *infile, char *lines[], int n);
long tail(FILE *fp, char *lines[], int n);
void eprint(int err, char *fmt, va_list args);
char *chopfmt(char *fmt);
void syserror(char *fmt, ...);
void sysfatal(char *fmt, ...);
void *LookupAndSend (int m_sock); 
char *GetLogDir();
char *GetLogList(char *logdate);
int GetEventsInOldLogs(char *logdate);
int strtoken(const char *s, char delim, char **token);
int InfoAdd(int id, char *value, const char * flag);
char *InfoGet(int id, const char * flag);

/* Variables initialization */

char *j2js[HASHSIZE];
char *j2ec[HASHSIZE];

char *bjl[HASHSIZE];

char *argv0;

int wlock=0;
int rcounter=0;

pthread_mutex_t read_mutex  = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;

char *lsbevents="lsb.events";
char *ldir;

char *LastLog=NULL;

char *blank=" ";

char * rex_queued    = "Job Queued ";
char * rex_running   = "Job Run ";
char * rex_deleted   = "Job deleted ";
char * rex_finished  = "Exit_status=";
char * rex_hold      = "Holds ";
