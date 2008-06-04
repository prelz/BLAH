#define _GNU_SOURCE
#define _XOPEN_SOURCE
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <time.h>
#include <sys/types.h> 
#include <sys/stat.h>
#include <popt.h>
#include <math.h>
#include "blahpd.h"

#define STR_CHARS          50000
#define NUM_CHARS          300
#define NUMTOK             20
#define ERRMAX             80

ssize_t Readline(int fd, void *vptr, size_t maxlen);
ssize_t Writeline(int fc, const void *vptr, size_t maxlen);
char *get_line(FILE * f);
int freetoken(char ***token, int maxtok);
int strtoken(const char *s, char delim, char ***token);
char *strdel(char *s, const char *delete);
char *epoch2str(char *epoch);
char *iepoch2str(int epoch);
int str2epoch(char *str, char *f);
void daemonize();
int writepid(char * pidfile);
void eprint(int err, char *fmt, va_list args);
char *chopfmt(char *fmt);
void syserror(char *fmt, ...);
void sysfatal(char *fmt, ...);

char *argv0;

#define BUPDATER_ACTIVE_JOBS_FAILURE -1
#define BUPDATER_ACTIVE_JOBS_SUCCESS 0

typedef struct bupdater_active_jobs_t
 {
  int    njobs;
  int    is_sorted;
  char **jobs;
 } bupdater_active_jobs;

int bupdater_push_active_job(bupdater_active_jobs *bact, const char *job_id);
void bupdater_sort_active_jobs(bupdater_active_jobs *bact, int left, int right);
int bupdater_lookup_active_jobs(bupdater_active_jobs *bact,
                                const char *job_id);
void bupdater_free_active_jobs(bupdater_active_jobs *bact);
