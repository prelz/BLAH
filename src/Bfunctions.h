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

#define STR_CHARS          5000
#define NUM_CHARS          300
#define CSTR_CHARS         25
#define ERRMAX             80

ssize_t Readline(int fd, void *vptr, size_t maxlen);
ssize_t Writeline(int fc, const void *vptr, size_t maxlen);
int strtoken(const char *s, char delim, char **token);
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
