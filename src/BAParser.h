#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <popt.h>
#include <globus_gss_assist.h>
#include <globus_gsi_proxy.h>
#include <globus_gsi_system_config.h>

#include "BPRcomm.h"

#define LISTENQ            1024
#define DEFAULT_PORT       43432
#define MAX_CHARS          100000
#define STR_CHARS          3000
#define NUMTHRDS           20
#define ERRMAX             80
#define TBUFSIZE           400

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

#define CHECK_GLOBUS_CALL(error_str, error_code, token_status) \
        if (major_status != GSS_S_COMPLETE) \
        { \
                globus_gss_assist_display_status( \
                                stderr, \
                                error_str, \
                                major_status, \
                                minor_status, \
                                token_status); \
                return (error_code); \
        }

ssize_t Readline(int fd, void *vptr, size_t maxlen);
ssize_t Writeline(int fc, const void *vptr, size_t maxlen);
void eprint(int err, char *fmt, va_list args);
char *chopfmt(char *fmt);
void syserror(char *fmt, ...);
void sysfatal(char *fmt, ...);
void *GetAndSend (int m_sock);
char *ParseDGASFile(char *jobid,char *lrms);
int WriteDN(char *jobid,char *lrms);
int ReadDN(char *jobid,char *lrms);
char *verify_dn(gss_ctx_id_t context_handle);
int strtoken(const char *s, char delim, char **token);
void daemonize();

char *argv0;
int port;
int debug=0;
int dmn=0;
static char *progname = "BAParser";

char *spooldir = "/opt/glite/var/dgasURBox";
char *dndir    = "/tmp";

gss_cred_id_t   credential_handle = GSS_C_NO_CREDENTIAL;
gss_ctx_id_t    context_handle    = GSS_C_NO_CONTEXT;
OM_uint32       major_status = 0, minor_status = 0;
char *client_name = NULL;

