#ifndef BLAHPD_H_INCLUDED
#define BLAHPD_H_INCLUDED

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include "config.h"

#define RCSID_VERSION		"$GahpVersion: %s Nov 30 2006 INFN\\ blahpd\\ (%s) $"
#define BUFFER_FILE		"/tmp/blahp_result_buffer"
#define FLUSHED_BUFFER		"/tmp/blahp_result_buffer.flushed"

#define DEFAULT_GLITE_LOCATION	"/opt/glite"
#define DEFAULT_GLEXEC_COMMAND "/opt/glite/sbin/glexec"

/* Change this in order to select the default batch system
 * (overridden by BLAH_LRMS env variable)*/
#define DEFAULT_LRMS	"lsf"

#define BLAHPD_CRLF	"\r\n"

#define POLL_INTERVAL	3

#define MALLOC_ERROR	1

#define JOBID_MAX_LEN	256
#define ERROR_MAX_LEN	256
#define RESLN_MAX_LEN	2048
#define MAX_JOB_NUMBER  10

#ifdef DEBUG
#define BLAHDBG(format, message) fprintf(stderr, format, message)
#else
#define BLAHDBG(format, message)
#endif

/* Job states
 * */
typedef enum job_states {
	UNEFINED,
	IDLE,
	RUNNING,
	REMOVED,
	COMPLETED,
	HELD
} job_status_t;

/* Struct for the result lines 
 * (single linked list)        
 * */
typedef struct s_resline {
	struct s_resline *next;
	char *text;
} t_resline;


char *make_message(const char *fmt, ...);

extern config_handle *blah_config_handle;

#endif /* defined BLAHPD_H_INCLUDED */
