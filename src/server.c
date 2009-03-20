/*
#  File:     server.c
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#   20 Mar 2004 - Original release.
#   23 Apr 2004 - Command parsing moved to a dedicated module.
#   25 Apr 2004 - Handling of Job arguments as list in classad
#                 Result buffer made persistent between sessions
#   29 Apr 2004 - Handling of Job arguments as list in classad removed
#                 Result buffer no longer persistant
#    7 May 2004 - 'Write' commands embedded in locks to avoid output corruption
#                 Uses dynamic strings to retrieve classad attributes' value
#   12 Jul 2004 - (prelz@mi.infn.it). Fixed quoting style for environment.
#   13 Jul 2004 - (prelz@mi.infn.it). Make sure an entire command is assembled 
#                                     before forwarding it.
#   20 Sep 2004 - Added support for Queue attribute.
#   12 Dec 2006 - (prelz@mi.infn.it). Added commands to cache proxy
#                 filenames.
#   23 Nov 2007 - (prelz@mi.infn.it). Access blah.config via config API.
#   31 Jan 2008 - (prelz@mi.infn.it). Add watches on a few needed processes
#                                     (bupdater and bnotifier for starters)
#   31 Mar 2008 - (rebatto@mi.infn.it) Async mode handling moved to resbuffer.c
#                                      Adapted to new resbuffer functions
#   17 Jun 2008 - (prelz@mi.infn.it). Add access to the job proxy stored
#                                     in the job registry.
#                                     Make job proxy optional.
#   15 Oct 2008 - (prelz@mi.infn.it). Make proxy renewal on worker nodes
#                                     optional via config.
#
#  Description:
#   Serve a connection to a blahp client, performing appropriate
#   job operations according to client requests.
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <regex.h>
#include <errno.h>
#include <netdb.h>
#include <libgen.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>

#include "blahpd.h"
#include "config.h"
#include "job_registry.h"
#include "classad_c_helper.h"
#include "commands.h"
#include "job_status.h"
#include "resbuffer.h"
#include "mtsafe_popen.h"
#include "proxy_hashcontainer.h"

#define COMMAND_PREFIX "-c"
#define PERSISTENT_BUFFER BUFFER_DONT_SAVE
#define JOBID_REGEXP            "(^|\n)BLAHP_JOBID_PREFIX([^\n]*)"
#define HOLD_JOB                1
#define RESUME_JOB              0
#define MAX_LRMS_NUMBER 	10
#define MAX_LRMS_NAME_SIZE	8
#define MAX_CERT_SIZE		100000
#define MAX_TEMP_ARRAY_SIZE              1000
#define MAX_FILE_LIST_BUFFER_SIZE        10000
#define MAX_PENDING_COMMANDS             500
#define DEFAULT_TEMP_DIR                 "/tmp"
 
#define NO_QUOTE     0
#define SINGLE_QUOTE 1
#define DOUBLE_QUOTE 2
#define INT_NOQUOTE  3

#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE  1
#endif

const char *opt_format[] = {
	" %s %s",               /* NO_QUOTE */
	" %s \"%s\"",           /* SINGLE_QUOTE */
	" %s \"\\\"%s\\\"\"",   /* DOUBLE_QUOTE */
	" %s %d"                /* INT_NOQUOTE */
};

const char *statusstring[] = {
 "IDLE",
 "RUNNING",
 "REMOVED",
 "IDLE",
 "HELD",
};

/* Function prototypes */
char *get_command(int client_socket);
char *escape_spaces(const char *str);
int set_cmd_list_option(char **command, classad_context cad, const char *attribute, const char *option);
int set_cmd_string_option(char **command, classad_context cad, const char *attribute, const char *option, const int quote_style);
int set_cmd_int_option(char **command, classad_context cad, const char *attribute, const char *option, const int quote_style);
int set_cmd_bool_option(char **command, classad_context cad, const char *attribute, const char *option, const int quote_style);
int limit_proxy(char* proxy_name, char* limited_proxy_name);
int getProxyInfo(char* proxname, char* fqan, char* userDN);
int logAccInfo(char* jobId, char* server_lrms, classad_context cad, char* fqan, char* userDN, char** environment);
int CEReq_parse(classad_context cad, char* filename);
char* outputfileRemaps(char *sb,char *sbrmp);
int check_TransferINOUT(classad_context cad, char **command, char *reqId);
char *ConvertArgs(char* args, char sep);

/* Global variables */
struct blah_managed_child {
	char *exefile;
	char *pidfile;
};
static struct blah_managed_child *blah_children=NULL;
static int blah_children_count=0;

config_handle *blah_config_handle = NULL;
job_registry_handle *blah_jr_handle = NULL;
static int server_socket;
static int exit_program = 0;
static pthread_mutex_t send_lock  = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t bfork_lock  = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t blah_jr_lock  = PTHREAD_MUTEX_INITIALIZER;
pthread_attr_t cmd_threads_attr;

sem_t sem_total_commands;

char *blah_script_location;
char *blah_version;
static char lrmslist[MAX_LRMS_NUMBER][MAX_LRMS_NAME_SIZE];
static int  lrms_counter = 0;
int  glexec_mode = 0; /* FIXME: need to become static */
char *tmp_dir;
struct stat tmp_stat;
char *bssp = NULL;
char *gloc = NULL;
int enable_condor_glexec = FALSE;
int require_proxy_on_submit = FALSE;
int disable_wn_proxy_renewal = FALSE;
static char *blah_omem_msg="Out\\ of\\ memory";

/* GLEXEC ENVIRONMENT VARIABLES */
#define GLEXEC_MODE_IDX         0
#define GLEXEC_CLIENT_CERT_IDX  1
#define GLEXEC_SOURCE_PROXY_IDX 2
#define GLEXEC_ENV_TOTAL        3
static char *glexec_env_name[] = {"GLEXEC_MODE", "GLEXEC_CLIENT_CERT", "GLEXEC_SOURCE_PROXY"};
static char *glexec_env_var[GLEXEC_ENV_TOTAL];

/* #define TSF_DEBUG */

/* Check on good health of our managed children
 **/
void
check_on_children(const struct blah_managed_child *children, const int count)
{
	FILE *pid;
	pid_t ch_pid;
	int junk;
	int i;
	int try_to_restart;
	int fret;
	static time_t lastfork=0;
	time_t new_lastfork;
	static time_t calldiff=0;
	const time_t default_calldiff = 150;
	config_entry *ccld;
	time_t now;

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
				/* Child process. Exec exe file. */
				if (execl(children[i].exefile, children[i].exefile, NULL) < 0)
				{
					fprintf(stderr,"Cannot exec %s: %s\n",
						children[i].exefile,
						strerror(errno));
					exit(1);
				}
			} else if (fret < 0) {
				fprintf(stderr,"Cannot fork trying to start %s: %s\n",
					children[i].exefile,
					strerror(errno));
			}
			new_lastfork = now;
		}
	}

	/* Reap dead children. Yuck.*/
	while (waitpid(-1, &junk, WNOHANG) > 0) /* Empty loop */;

	lastfork = new_lastfork;

	pthread_mutex_unlock(&bfork_lock);
}

/* Free all tokens of a command
 * */
void
free_args(char **arg_array)
{
	char **arg_ptr;
	
	if (arg_array)
	{
		for (arg_ptr = arg_array; (*arg_ptr) != NULL; arg_ptr++)
			free(*arg_ptr);
		free(arg_array);
	}
}	
/* Main server function 
 * */
int
serveConnection(int cli_socket, char* cli_ip_addr)
{
	char *input_buffer;
	char *reply;
	char *result;
	char *cmd_result;
	fd_set readfs;
	int exitcode = 0;
	int reply_len;
	pthread_t task_tid;
	int i, argc;
	char **argv;
	command_t *command;
	int bc=0;
	char *needed_libs=NULL;
	char *old_ld_lib=NULL;
	char *new_ld_lib=NULL;
	config_entry *suplrms, *jre;
	char *next_lrms_s, *next_lrms_e;
	int lrms_len;
	char *children_names[3] = {"bupdater", "bnotifier", NULL};
	char **child_prefix;
	char *child_exe_conf, *child_pid_conf;
        config_entry *child_config_exe, *child_config_pid;
	int max_threaded_cmds = MAX_PENDING_COMMANDS;
	config_entry *max_threaded_conf;

	blah_config_handle = config_read(NULL);
	if (blah_config_handle == NULL)
	{
		fprintf(stderr, "Cannot access blah.config file in default locations ($GLITE_LOCATION/etc or $BLAHPD_LOCATION/etc): ");
		perror("");
		exit(MALLOC_ERROR);
	}

	max_threaded_conf = config_get("blah_max_threaded_cmds",blah_config_handle);
	if (max_threaded_conf != NULL) max_threaded_cmds = atoi(max_threaded_conf->value);

	for (i = 0; i < GLEXEC_ENV_TOTAL; i++)
		glexec_env_var[i] = NULL;

	init_resbuffer();
	if (cli_socket == 0) server_socket = 1;
	else                 server_socket = cli_socket;

	/* Get values from environment */
	if ((result = getenv("GLITE_LOCATION")) == NULL)
	{
		result = DEFAULT_GLITE_LOCATION;
	}
        if ((tmp_dir = getenv("GAHP_TEMP")) == NULL)
        {
                tmp_dir  = DEFAULT_TEMP_DIR;
        }

	needed_libs = make_message("%s/lib:%s/externals/lib:%s/lib:/opt/lcg/lib", result, result, getenv("GLOBUS_LOCATION") ? getenv("GLOBUS_LOCATION") : "/opt/globus");
	old_ld_lib=getenv("LD_LIBRARY_PATH");
	if(old_ld_lib)
	{
		new_ld_lib =(char*)malloc(strlen(old_ld_lib) + 2 + strlen(needed_libs));
		if (new_ld_lib == NULL)
		{
			fprintf(stderr, "Out of memory\n");
			exit(MALLOC_ERROR);
		}
	  	sprintf(new_ld_lib,"%s;%s",old_ld_lib,needed_libs);
	  	setenv("LD_LIBRARY_PATH",new_ld_lib,1);
	}else
	 	 setenv("LD_LIBRARY_PATH",needed_libs,1);
	
	blah_script_location = strdup(blah_config_handle->bin_path);
	blah_version = make_message(RCSID_VERSION, VERSION, "poly,new_esc_format");
	if ((gloc=getenv("GLEXEC_COMMAND")) == NULL)
	{
		gloc = DEFAULT_GLEXEC_COMMAND;
	}
	require_proxy_on_submit = config_test_boolean(config_get("blah_require_proxy_on_submit",blah_config_handle));
	enable_condor_glexec = config_test_boolean(config_get("blah_enable_glexec_from_condor",blah_config_handle));
	disable_wn_proxy_renewal = config_test_boolean(config_get("blah_disable_wn_proxy_renewal",blah_config_handle));
				
	if (enable_condor_glexec)
	{
		/* Enable condor/glexec commands */
		/* FIXME: should check/assert for success */
		command = find_command("CACHE_PROXY_FROM_FILE");
		if (command) command->cmd_handler = cmd_cache_proxy_from_file;
		command = find_command("USE_CACHED_PROXY");
		if (command) command->cmd_handler = cmd_use_cached_proxy;
		command = find_command("UNCACHE_PROXY");
		if (command) command->cmd_handler = cmd_uncache_proxy;
		/* Check that tmp_dir is group writable */
		if (stat(tmp_dir, &tmp_stat) >= 0)
		{
			if ((tmp_stat.st_mode & S_IWGRP) == 0)
			{
				if (chmod(tmp_dir,tmp_stat.st_mode|S_IWGRP|S_IRGRP|S_IXGRP)<0)
				{
					fprintf(stderr,"WARNING: cannot make %s group writable: %s\n",
					        tmp_dir, strerror(errno));
				}
			}
		}
	}

	suplrms = config_get("supported_lrms", blah_config_handle);

	if (suplrms != NULL)
	{
		next_lrms_s = suplrms->value;
		for(lrms_counter = 0; next_lrms_s < (suplrms->value + strlen(suplrms->value)) && lrms_counter < MAX_LRMS_NUMBER; )
		{
			next_lrms_e = strchr(next_lrms_s,','); 
			if (next_lrms_e == NULL) 
				next_lrms_e = suplrms->value + strlen(suplrms->value);
			lrms_len = (int)(next_lrms_e - next_lrms_s);
			if (lrms_len >= MAX_LRMS_NAME_SIZE)
				lrms_len = MAX_LRMS_NAME_SIZE-1;
			if (lrms_len > 0)
			{
				memcpy(lrmslist[lrms_counter],next_lrms_s,lrms_len);
				lrmslist[lrms_counter][lrms_len] = '\000';
				lrms_counter++;
			}
			if (*next_lrms_e == '\000') break;
			next_lrms_s = next_lrms_e + 1;
		}
	}
	
	jre = config_get("job_registry", blah_config_handle);
	if (jre != NULL)
	{
		blah_jr_handle = job_registry_init(jre->value, BY_BLAH_ID);
		if (blah_jr_handle != NULL)
		{
			/* Enable BLAH_JOB_STATUS_ALL/SELECT commands */
                        /* (served by the same function) */
			/* FIXME: should check/assert for success */
			command = find_command("BLAH_JOB_STATUS_ALL");
			if (command) command->cmd_handler = cmd_status_job_all;
			command = find_command("BLAH_JOB_STATUS_SELECT");
			if (command) command->cmd_handler = cmd_status_job_all; 
		}
	}

	/* Get list of BLAHPD children whose survival we care about. */

	blah_children_count = 0;
	for (child_prefix = children_names; (*child_prefix)!=NULL;
	     child_prefix++)
	{
		child_pid_conf = make_message("%s_pidfile",*child_prefix);
		child_exe_conf = make_message("%s_path",*child_prefix);
		if (child_exe_conf == NULL || child_pid_conf == NULL)
		{
			fprintf(stderr, "Out of memory\n");
			exit(MALLOC_ERROR);
		}
		child_config_exe = config_get(child_exe_conf,blah_config_handle);
		child_config_pid = config_get(child_pid_conf,blah_config_handle);
		
		if (child_config_exe != NULL && child_config_pid != NULL)
		{
			if (strlen(child_config_exe->value) <= 0 ||
			    strlen(child_config_pid->value) <= 0) 
			{
				free(child_exe_conf);
				free(child_pid_conf);
				continue;
			}

			blah_children = realloc(blah_children,
				(blah_children_count + 1) * 
				sizeof(struct blah_managed_child));
			if (blah_children == NULL)
			{
				fprintf(stderr, "Out of memory\n");
				exit(MALLOC_ERROR);
			}
			blah_children[blah_children_count].exefile = strdup(child_config_exe->value);
			blah_children[blah_children_count].pidfile = strdup(child_config_pid->value);
			if (blah_children[blah_children_count].exefile == NULL ||
			    blah_children[blah_children_count].pidfile == NULL)
			{
				fprintf(stderr, "Out of memory\n");
				exit(MALLOC_ERROR);
			}
			blah_children_count++;
			
		}
		free(child_exe_conf);
		free(child_pid_conf);
	}

	if (blah_children_count>0) check_on_children(blah_children, blah_children_count);

	pthread_attr_init(&cmd_threads_attr);
	pthread_attr_setdetachstate(&cmd_threads_attr, PTHREAD_CREATE_DETACHED);

	sem_init(&sem_total_commands, 0, max_threaded_cmds);
	
	write(server_socket, blah_version, strlen(blah_version));
	write(server_socket, "\r\n", 2);
	while(!exit_program)
	{
		input_buffer = get_command(cli_socket);
		if (input_buffer)
		{
			if (parse_command(input_buffer, &argc, &argv) == 0)
				command = find_command(argv[0]);
			else
				command = NULL;

			if (command && (command->cmd_handler == cmd_unknown))
				command = NULL;

			if (command)
			{
				if (argc != (command->required_params + 1))
				{
					reply = make_message("E expected\\ %d\\ parameters,\\ %d\\ found\\r\n", command->required_params, argc-1);
				}
				else
				{
					if (command->threaded)
					{	
						/* Add the glexec environment as last arguments */
						if (glexec_mode) 
						{
							argv = (char **)realloc(argv, (argc + GLEXEC_ENV_TOTAL + 1) * sizeof(char *));
							for (i = 0; i < GLEXEC_ENV_TOTAL; i++) 
							{
								argv[argc + i] = make_message("%s=%s", glexec_env_name[i], glexec_env_var[i]);
							}
							argv[argc + GLEXEC_ENV_TOTAL] = NULL;
						}

						if (sem_trywait(&sem_total_commands))
						{
							reply = make_message("F Threads\\ limit\\ reached\r\n");
						}
						else if (pthread_create(&task_tid, &cmd_threads_attr, command->cmd_handler, (void *)argv))
						{
							reply = make_message("F Cannot\\ start\\ thread\r\n");
						}
						else
						{
							reply = make_message("S\r\n");
						}
						/* free argv in threaded function */
					}
					else
					{
						cmd_result = (char *)command->cmd_handler(argv);
						reply = make_message("%s\r\n", cmd_result);
						free(cmd_result);
						free_args(argv);
					}
				}
			}
			else
			{
				reply = make_message("E Unknown\\ command\r\n");
				free_args(argv);
			}

			pthread_mutex_lock(&send_lock);
			if (reply)
			{
				if (write(server_socket, reply, strlen(reply)) > 0)
					remove(FLUSHED_BUFFER);
				free(reply);
			}
			else
				/* WARNING: the command here could have been actually executed */
				write(server_socket, "F Cannot\\ allocate\\ return\\ line\r\n", 34);
			pthread_mutex_unlock(&send_lock);
			
			free(input_buffer);
		}
		else /* command was NULL */
		{
			fprintf(stderr, "Connection closed by remote host\n");
			exitcode = 1;
			break;
		}

	}
	if (cli_socket != 0) 
	{
		shutdown(cli_socket, SHUT_RDWR);
		close(cli_socket);
	}

	free(blah_script_location);
	free(blah_version);

	exit(exitcode);
}

/* Non threaded commands
 * --------
 * must return a pointer to a free-able string or NULL
 * */

void *
cmd_quit(void *args)
{
	char *result = NULL;
	
	exit_program = 1;
	result = strdup("S Server\\ exiting");
	return(result);
}

void *
cmd_commands(void *args)
{
    char *commands;
	char *result;

	if ((commands = known_commands()) != NULL)
	{
		result = make_message("S %s", commands);
		free(commands);
	}
	else
    {
		result = strdup("F known_commands()\\ returned\\ NULL");
	}
	return(result);
}

void *
cmd_unknown(void *args)
{
	/* Placeholder for commands that are not advertised via */
        /* known_commands and should therefore never be executed */
	return(NULL);
}
 
void *
cmd_cache_proxy_from_file(void *args)
{
	char **argv = (char **)args;
	char *proxyId = argv[1];
	char *proxyPath = argv[2];
	char *result;

	if (proxy_hashcontainer_append(proxyId, proxyPath) == NULL)
	{
		/* Error in insertion */
		result = strdup("F Internal\\ hash\\ error:\\ proxy\\ not\\ cached");
	} else 
	{
		result = strdup("S Proxy\\ cached");
	}
	return(result);
}

void *
cmd_use_cached_proxy(void *args)
{
	char **argv = (char **)args;
	char *glexec_argv[4];
	proxy_hashcontainer_entry *entry;
	char *proxyId = argv[1];
	char *result = NULL;
	char *junk;
	char *proxy_name, *proxy_dir, *escaped_proxy;
	int need_to_free_escaped_proxy = 0;
	struct stat proxy_stat;

	entry = proxy_hashcontainer_lookup(proxyId);
	if (entry == NULL)
	{
        result = strdup("F Cannot\\ find\\ required\\ proxyId");
	} else
	{
		glexec_argv[0] = argv[0];
		glexec_argv[1] = entry->proxy_file_name;
		glexec_argv[2] = entry->proxy_file_name;
		glexec_argv[3] = "0"; /* Limit the proxy */

		junk = (char *)cmd_set_glexec_dn((void *)glexec_argv);
		if (junk == NULL) 
		{
			result = strdup("F Glexec\\ proxy\\ not \\set\\ (internal\\ error)");
		} 
		else if (junk[0] == 'S')
		{ 
			free(junk);
			/* Check that the proxy dir is group writable */
			escaped_proxy = escape_spaces(entry->proxy_file_name);
			/* Out of memory: Try with the full name */
			if (escaped_proxy == NULL) escaped_proxy = entry->proxy_file_name;
			else need_to_free_escaped_proxy = 1;

			proxy_name = strdup(entry->proxy_file_name);
			if (proxy_name != NULL)
			{
				proxy_dir = dirname(proxy_name);
				if (stat(proxy_dir, &proxy_stat) >= 0)
				{
					if ((proxy_stat.st_mode & S_IWGRP) == 0)
					{
						if (chmod(proxy_dir,proxy_stat.st_mode|S_IWGRP|S_IRGRP|S_IXGRP)<0)
						{
							result = make_message("S Glexec\\ proxy\\ set\\ to\\ %s\\ (could\\ not\\ set\\ dir\\ IWGRP\\ bit)",
									escaped_proxy);
						} else {
							result = make_message("S Glexec\\ proxy\\ set\\ to\\ %s\\ (dir\\ IWGRP\\ bit\\ set)",
									escaped_proxy);
						}
					} else {
						result = make_message("S Glexec\\ proxy\\ set\\ to\\ %s\\ (dir\\ IWGRP\\ bit\\ already\\ on)",
									escaped_proxy);
					}
				} else {
					result = make_message("S Glexec\\ proxy\\ set\\ to\\ %s\\ (cannot\\ stat\\ dirname)",
								escaped_proxy);

				}
				free(proxy_name);
			} else {
				result = make_message("S Glexec\\ proxy\\ set\\ to\\ %s\\ (Out\\ of\\ memory)",
							escaped_proxy);
			}
		} else {
            /* Return original cmd_set_glexec_dn() error */
			result = junk;
		}
	}
	if (need_to_free_escaped_proxy) free(escaped_proxy);
	return(result);
}

void *
cmd_uncache_proxy(void *args)
{
	char **argv = (char **)args;
	char *proxyId = argv[1];
	char *junk;
	char *result = NULL;

	proxy_hashcontainer_entry *entry;

	entry = proxy_hashcontainer_lookup(proxyId);
	if (entry == NULL)
	{
		result = strdup("F proxyId\\ not\\ found");
	} else
	{
                      /* Check for entry->proxy_file_name at the beginning */
                      /* of glexec_env_var[GLEXEC_SOURCE_PROXY_IDX] to */
                      /* allow for limited proxies */
		if ( glexec_mode &&
		    ( strncmp(glexec_env_var[GLEXEC_SOURCE_PROXY_IDX],
		              entry->proxy_file_name,
                              strlen(entry->proxy_file_name)) == 0) )
		{
			/* Need to unregister cached proxy from glexec */
                	junk = (char *)cmd_unset_glexec_dn(NULL);
					/* FIXME: need to check for error message in junk? */
                	if (junk != NULL) free(junk);
		}
		proxy_hashcontainer_unlink(proxyId);
		result = strdup("S Proxy\\ uncached");
	}
	return(result);
}

void *
cmd_version(void *args)
{
	char *result;

	result = make_message("S %s",blah_version);
	return(result);	
}

void *
cmd_async_on(void *args)
{
	char *result;

	set_async_mode(ASYNC_MODE_ON);
	result = strdup("S Async\\ mode\\ on");
	return(result);
}
			
void *
cmd_async_off(void *args)
{
	char *result;

	set_async_mode(ASYNC_MODE_OFF);
	result = strdup("S Async\\ mode\\ off");
	return(result);
}

void *
cmd_results(void *args)
{
	char *result;

	result = get_lines();
	return(result);
}

void *
cmd_unset_glexec_dn(void *args)
{
	char *result;
	int i;

	for (i = 0; i < GLEXEC_ENV_TOTAL; i++)
		if (glexec_env_var[i])
		{
			free(glexec_env_var[i]);
			glexec_env_var[i] = NULL;
		}
	glexec_mode = 0;

	result = make_message("S Glexec\\ mode\\ off");
	return (result);
}

void *
cmd_set_glexec_dn(void *args)
{

	char *result  = NULL;
	struct stat buf;
	char **argv = (char **)args;
	char *proxt4= argv[1];
	char *ssl_client_cert = argv[2];
	char *dummy;
	int res = 0;
	char *proxynameNew = NULL;
	
	if (glexec_mode)
	{
		dummy = (char *)cmd_unset_glexec_dn(NULL);
		free(dummy);
	}

	if((!stat(proxt4, &buf)) && (!stat(ssl_client_cert, &buf)))
	{
		glexec_env_var[GLEXEC_MODE_IDX] = strdup("lcmaps_get_account");
		glexec_env_var[GLEXEC_CLIENT_CERT_IDX] = strdup(ssl_client_cert);
		/* proxt4 must be limited for subsequent submission */		
		if(argv[3][0]=='0')
		{
			proxynameNew = make_message("%s.lmt", proxt4);
			if(res = limit_proxy(proxt4, proxynameNew))
			{
				free(proxynameNew);
				free(glexec_env_var[GLEXEC_MODE_IDX]);
				free(glexec_env_var[GLEXEC_CLIENT_CERT_IDX]);
				glexec_env_var[GLEXEC_MODE_IDX] = NULL;
				glexec_env_var[GLEXEC_CLIENT_CERT_IDX] = NULL;
				result = strdup("F Cannot\\ limit\\ proxy\\ file");
			}
			else
				glexec_env_var[GLEXEC_SOURCE_PROXY_IDX] = proxynameNew;
		}
		else
		{
			glexec_env_var[GLEXEC_SOURCE_PROXY_IDX] = strdup(proxt4);
		}
		if (!result)
		{
			glexec_mode = 1;
			result = strdup("S Glexec\\ mode\\ on");
		}
	}
	else
	{
		result = strdup("F Cannot\\ stat\\ proxy\\ file");
	}
	return(result);
}

/* Threaded commands
 * N.B.: functions must free argv before return
 * */

#define CMD_SUBMIT_JOB_ARGS 2
void *
cmd_submit_job(void *args)
{
	char *cmd_out = NULL;
	char *cmd_err = NULL;
	char *escpd_cmd_out, *escpd_cmd_err;
	int retcod;
	char *command;
	char jobId[JOBID_MAX_LEN];
	char *resultLine=NULL;
	char **argv = (char **)args;
	classad_context cad;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	char *server_lrms = NULL;
	int result;
	char error_message[ERROR_MAX_LEN];
	char *error_string;
	int res = 1;
	char* resfg = NULL;
	char *proxyname = NULL;
	char *proxynameNew   = NULL;
	char *log_proxy;
	char *cmdstr = NULL;
	char *command_ext = NULL;
	char fqan[MAX_TEMP_ARRAY_SIZE], userDN[MAX_TEMP_ARRAY_SIZE];
	int count = 0;
	int enable_log = 0;
	struct timeval ts;
	char *ce_req=NULL;
	char *req_file=NULL;
	regex_t regbuf;
	size_t nmatch;
	regmatch_t pmatch[3];
        char *arguments=NULL;
        char *conv_arguments=NULL;
        char *environment=NULL;
        char *conv_environment=NULL;
	struct stat buf;


	/* Parse the job description classad */
	if ((cad = classad_parse(jobDescr)) == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Error\\ parsing\\ classad N/A", reqId);
		goto cleanup_argv;
	}

	/* Get the lrms type from classad attribute "gridtype" */
	if (classad_get_dstring_attribute(cad, "gridtype", &server_lrms) != C_CLASSAD_NO_ERROR)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Missing\\ gridtype\\ in\\ submission\\ classAd N/A", reqId);
		goto cleanup_cad;
	}

	/* Get the proxy name from classad attribute "X509UserProxy" */
	if (classad_get_dstring_attribute(cad, "x509UserProxy", &proxyname) != C_CLASSAD_NO_ERROR)
	{
		if (require_proxy_on_submit)
		{
			/* PUSH A FAILURE */
			resultLine = make_message("%s 1 Missing\\ x509UserProxy\\ in\\ submission\\ classAd N/A", reqId);
			goto cleanup_lrms;
		} else {
			proxyname = NULL;
		}
	}

	/* If there are additional arguments, we are in glexec mode */
	if(argv[CMD_SUBMIT_JOB_ARGS + 1] != NULL)
	{
		if (proxyname != NULL)
		{
			/* Add the target proxy - cause glexec to move it to another file */
			proxynameNew = make_message("%s.glexec", proxyname);
			free(proxyname);
			proxyname = proxynameNew;
                	/* Savannah #37003: Let glexec rotate the new proxy in place if (stat(proxynameNew, &buf) >= 0) unlink(proxynameNew); */

			for(count = CMD_SUBMIT_JOB_ARGS + 2; argv[count]; count++);
			argv = (char **)realloc(argv, sizeof(char *) * (count + 2));
			argv[count] = make_message("GLEXEC_TARGET_PROXY=%s", proxyname);
			argv[count + 1] = NULL;
			log_proxy = argv[CMD_SUBMIT_JOB_ARGS + 1 + GLEXEC_SOURCE_PROXY_IDX] + strlen(glexec_env_name[GLEXEC_SOURCE_PROXY_IDX]) + 1;
		}
	}
	else if (proxyname != NULL)
	{
		/* not in glexec mode: need to limit the proxy */
		proxynameNew = make_message("%s.lmt", proxyname);
		if(limit_proxy(proxyname, proxynameNew))
		{
			/* PUSH A FAILURE */
			resultLine = make_message("%s 1 Unable\\ to\\ limit\\ the\\ proxy N/A", reqId);
			free(proxynameNew);
			goto cleanup_proxyname;
		}
		free(proxyname);
		proxyname = proxynameNew;
		log_proxy = proxynameNew;
	}

	if (proxyname != NULL)
	{
		/* DGAS accounting */
		if (getProxyInfo(log_proxy, fqan, userDN))
		{
			/* PUSH A FAILURE */
			resultLine = make_message("%s 1 Credentials\\ not\\ valid N/A", reqId);
			goto cleanup_command;
		}
		if (userDN) enable_log=1;
	}

	command = make_message("%s %s/%s_submit.sh", argv[CMD_SUBMIT_JOB_ARGS + 1] ? gloc : "",
	                        blah_script_location, server_lrms);
	if (command == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Out\\ of\\ Memory N/A", reqId);
		goto cleanup_proxyname;
	}

        /* add proxy name if present */
	if (proxyname != NULL)
	{
		command_ext = make_message("%s -x %s", command, proxyname);
		if (command_ext == NULL)
		{
			/* PUSH A FAILURE */
			resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
			goto cleanup_command;
		}
		/* Swap new command in */
		free(command);
		command = command_ext;
	}

        /* Add command line option to explicitely disable proxy renewal */
        /* if requested. */
	if (disable_wn_proxy_renewal)
	{
		command_ext = make_message("%s -r no", command);
		if (command_ext == NULL)
		{
			/* PUSH A FAILURE */
			resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
			goto cleanup_command;
		}
		/* Swap new command in */
		free(command);
		command = command_ext;
	}

	/* Cmd attribute is mandatory: stop on any error */
	if (set_cmd_string_option(&command, cad, "Cmd", COMMAND_PREFIX, NO_QUOTE) != C_CLASSAD_NO_ERROR)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 7 Cannot\\ parse\\ Cmd\\ attribute\\ in\\ classad N/A", reqId);
		goto cleanup_command;
	}

        /* temporary directory path*/
	command_ext = make_message("%s -T %s", command, tmp_dir);
	if (command_ext == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
		goto cleanup_command;
	}
	/* Swap new command in */
	free(command);
	command = command_ext;
	if(check_TransferINOUT(cad,&command,reqId))
		goto cleanup_cad;

	/* Set the CE requirements */
	if((result = classad_get_dstring_attribute(cad, "CERequirements", &ce_req)) == C_CLASSAD_NO_ERROR)
	{
		gettimeofday(&ts, NULL);
		req_file = make_message("%s/ce-req-file-%d%d",tmp_dir, ts.tv_sec, ts.tv_usec);
		if(!CEReq_parse(cad, req_file))
		{
			command_ext = make_message("%s -C %s", command, req_file);
			if (command_ext == NULL)
			{
				/* PUSH A FAILURE */
				resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
				free(req_file);
				goto cleanup_command;
			}
			/* Swap new command in */
			free(command);
			command = command_ext;
		}
		free(req_file);
	}

	/* All other attributes are optional: fail only on memory error 
	   IMPORTANT: Args must alway be the last!
	*/
	if ((set_cmd_string_option(&command, cad, "In",         "-i", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Out",        "-o", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Err",        "-e", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Iwd",        "-w", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
//	    (set_cmd_string_option(&command, cad, "Env",        "-v", SINGLE_QUOTE)  == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Queue",      "-q", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_int_option   (&command, cad, "NodeNumber", "-n", INT_NOQUOTE)   == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_bool_option  (&command, cad, "StageCmd",   "-s", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "ClientJobId","-j", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY))
//	    (set_cmd_string_option(&command, cad, "Args",      	"--", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY))
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
		goto cleanup_command;
	}

        /*if present environment attribute must be used instead of env */
        if ((result = classad_get_dstring_attribute(cad, "environment", &environment)) == C_CLASSAD_NO_ERROR)
        {
		if (environment[0] != '\000')
		{
                	conv_environment = ConvertArgs(environment, ' ');
			free(environment);
			/* fprintf(stderr, "DEBUG: args conversion <%s> to <%s>\n", environment, conv_environment); */
                	if (conv_environment)
                	{
				command_ext = make_message("%s -V %s", command, conv_environment);
				free(conv_environment);
			}

			if ((conv_environment == NULL) || (command_ext == NULL))
			{
				/* PUSH A FAILURE */
                                resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
				goto cleanup_command;
			}
			/* Swap new command in */
			free(command);
			command = command_ext;
		}
        }else
        if(set_cmd_string_option(&command, cad, "Env","-v", SINGLE_QUOTE) == C_CLASSAD_OUT_OF_MEMORY)
        {
                /* PUSH A FAILURE */
                resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
                goto cleanup_command;
        }

        /*if present arguments attribute must be used instead of args */
        if ((result = classad_get_dstring_attribute(cad, "Arguments", &arguments)) == C_CLASSAD_NO_ERROR)
        {
		if (arguments[0] != '\000')
		{
                	conv_arguments = ConvertArgs(arguments, ' ');
			free(arguments);
			/* fprintf(stderr, "DEBUG: args conversion <%s> to <%s>\n", arguments, conv_arguments); */
                	if (conv_arguments)
                	{
				command_ext = make_message("%s -- %s", command, conv_arguments);
				free(conv_arguments);
			}

			if ((conv_arguments == NULL) || (command_ext == NULL))
			{
				/* PUSH A FAILURE */
				resultLine = make_message("%s 1 Out\\ of\\ memory\\ creating\\ submission\\ command N/A", reqId);
				goto cleanup_command;
			}
			/* Swap new command in */
			free(command);
			command = command_ext;
		}
	}
	else if (set_cmd_string_option(&command, cad, "Args","--", NO_QUOTE) == C_CLASSAD_OUT_OF_MEMORY)
        {
		/* PUSH A FAILURE */
                resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
		goto cleanup_command;
	}

	/* Execute the submission command */
	retcod = exe_getouterr(command, argv + CMD_SUBMIT_JOB_ARGS + 1, &cmd_out, &cmd_err);

	if (retcod != 0)
	{
		/* PUSH A FAILURE */
		escpd_cmd_out = escape_spaces(cmd_out);
		if (escpd_cmd_out == NULL) escpd_cmd_out = blah_omem_msg;
		escpd_cmd_err = escape_spaces(cmd_err);
		if (escpd_cmd_err == NULL) escpd_cmd_err = blah_omem_msg;
		resultLine = make_message("%s %d submission\\ command\\ failed\\ (exit\\ code\\ =\\ %d)\\ (stdout:%s)\\ (stderr:%s) N/A", reqId, retcod, retcod, escpd_cmd_out, escpd_cmd_err);
		if (escpd_cmd_out != blah_omem_msg) free(escpd_cmd_out);
		if (escpd_cmd_err != blah_omem_msg) free(escpd_cmd_err);
		goto cleanup_cmd_out;
	}


	if (regcomp(&regbuf, JOBID_REGEXP, REG_EXTENDED) != 0)
	{
		/* this mean a bug in the regexp, cannot do anything further */
		fprintf(stderr, "Fatal: cannot compile regexp\n");
		exit(1);
	}

	if (regexec(&regbuf, cmd_out, 3, pmatch, 0) != 0)
	{
		/* PUSH A FAILURE */
		escpd_cmd_out = escape_spaces(cmd_out);
		if (escpd_cmd_out == NULL) escpd_cmd_out = blah_omem_msg;
		escpd_cmd_err = escape_spaces(cmd_err);
		if (escpd_cmd_err == NULL) escpd_cmd_err = blah_omem_msg;
		resultLine = make_message("%s 8 no\\ jobId\\ in\\ submission\\ script's\\ output\\ (stdout:%s)\\ (stderr:%s) N/A", reqId, escpd_cmd_out, escpd_cmd_err);
		if (escpd_cmd_out != blah_omem_msg) free(escpd_cmd_out);
		if (escpd_cmd_err != blah_omem_msg) free(escpd_cmd_err);
		goto cleanup_cmd_out;
	}

	cmd_out[pmatch[2].rm_eo] = '\000';
	strncpy(jobId, cmd_out + pmatch[2].rm_so, sizeof(jobId));

	/* PUSH A SUCCESS */
	resultLine = make_message("%s 0 No\\ error %s", reqId, jobId);
	
	/* DGAS accounting */
	if (enable_log)
		logAccInfo(jobId, server_lrms, cad, fqan, userDN, argv + CMD_SUBMIT_JOB_ARGS + 1);

	/* Free up all arguments and exit (exit point in case of error is the label
	   pointing to last successfully allocated variable) */
cleanup_cmd_out:
	free(cmd_out);
	if (cmd_err) free(cmd_err);
	regfree(&regbuf);
cleanup_command:
	free(command);
cleanup_proxyname:
	if (proxyname != NULL) free(proxyname);
cleanup_lrms:
	free(server_lrms);
cleanup_cad:
	classad_free(cad);
cleanup_argv:
	free_args(argv);
	if (resultLine)
	{
		enqueue_result(resultLine);
		free(resultLine);
	}
	else
	{
		fprintf(stderr, "blahpd: out of memory! Exiting...\n");
		exit(MALLOC_ERROR);
	}
	sem_post(&sem_total_commands);
	return;
}

#define CMD_CANCEL_JOB_ARGS 2
void *
cmd_cancel_job(void* args)
{
	int retcod;
	char *cmd_out, *cmd_err;
	char *escpd_cmd_out, *escpd_cmd_err;
	char *begin_res;
	char *end_res;
	int res_length;
	char *command;
	char *resultLine = NULL;
	char **argv = (char **)args;
	char **arg_ptr;
	job_registry_split_id *spid;
	char *reqId = argv[1];
	char *error_string;
	char answer[1024];

	/* Split <lrms> and actual job Id */
	if((spid = job_registry_split_blah_id(argv[2])) == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 2 Malformed\\ jobId\\ %s\\ or\\ out\\ of\\ memory", reqId, argv[2]);
		goto cleanup_argv;
	}

	/* Prepare the cancellation command */
	command = make_message("%s %s/%s_cancel.sh %s", argv[CMD_CANCEL_JOB_ARGS + 1] ? gloc : "", blah_script_location, spid->lrms, spid->script_id);
	if (command == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Cannot\\ allocate\\ memory\\ for\\ the\\ command\\ string", reqId);
		goto cleanup_lrms;
	}

	/* Execute the command */
	if (retcod = exe_getouterr(command, argv + CMD_CANCEL_JOB_ARGS + 1, &cmd_out, &cmd_err))
	{
		/* PUSH A FAILURE */
		escpd_cmd_out = escape_spaces(cmd_out);
		if (escpd_cmd_out == NULL) escpd_cmd_out = blah_omem_msg;
		escpd_cmd_err = escape_spaces(cmd_err);
		if (escpd_cmd_err == NULL) escpd_cmd_err = blah_omem_msg;
		resultLine = make_message("%s %d Cancellation\\ command\\ failed\\ (stdout:%s)\\ (stderr:%s)", reqId, retcod, escpd_cmd_out, escpd_cmd_err);
		if (escpd_cmd_out != blah_omem_msg) free(escpd_cmd_out);
		if (escpd_cmd_err != blah_omem_msg) free(escpd_cmd_err);
		goto cleanup_command;
	}	

	/* Multiple job cancellation */
	res_length = strlen(cmd_out);
	for (begin_res = cmd_out; end_res = memchr(cmd_out, '\n', res_length); begin_res = end_res + 1)
	{
		*end_res = 0;
		resultLine = make_message("%s%s", reqId, begin_res);
		enqueue_result(resultLine);
		free(resultLine);
		resultLine = NULL;
	}

	/* Free up all arguments and exit (exit point in case of error is the label
	   pointing to last successfully allocated variable) */
cleanup_command:
	if (cmd_out) free(cmd_out);
	if (cmd_err) free(cmd_err);
	free(command);
cleanup_lrms:
	job_registry_free_split_id(spid);
cleanup_argv:
	free_args(argv);
	if(resultLine)
	{
		enqueue_result(resultLine);
		free (resultLine);
	}
	sem_post(&sem_total_commands);
	return;
}

#define CMD_STATUS_JOB_ARGS 2
void*
cmd_status_job(void *args)
{
	classad_context status_ad[MAX_JOB_NUMBER];
	char *str_cad;
	char *esc_str_cad;
	char *resultLine;
	char **argv = (char **)args;
	char **arg_ptr;
	char errstr[MAX_JOB_NUMBER][ERROR_MAX_LEN];
	char *esc_errstr;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	int jobStatus, retcode;
	int i, job_number;

	if (blah_children_count>0) check_on_children(blah_children, blah_children_count);

	retcode = get_status(jobDescr, status_ad, argv + CMD_STATUS_JOB_ARGS + 1, errstr, 0, &job_number);
	if (!retcode)
	{
		for(i = 0; i < job_number; i++)
		{
			if (status_ad[i] != NULL)
			{
				classad_get_int_attribute(status_ad[i], "JobStatus", &jobStatus);
				str_cad = classad_unparse(status_ad[i]);
				esc_str_cad = escape_spaces(str_cad);
				esc_errstr = escape_spaces(errstr[i]);
				if (job_number > 1)
					resultLine = make_message("%s.%d %d %s %d %s", reqId, i, retcode, esc_errstr, jobStatus, esc_str_cad);
				else
					resultLine = make_message("%s %d %s %d %s", reqId, retcode, esc_errstr, jobStatus, esc_str_cad);
				classad_free(status_ad[i]);
				free(str_cad);
				free(esc_str_cad);
			}
			else
			{
				esc_errstr = escape_spaces(errstr[i]);
				if(job_number > 1)
					resultLine = make_message("%s.%d 1 %s 0 N/A", reqId, i, esc_errstr);
				else
					resultLine = make_message("%s 1 %s 0 N/A", reqId, esc_errstr);
			}
			free(esc_errstr);
			enqueue_result(resultLine);
			free(resultLine);
		}
	}
	else
	{
		esc_errstr = escape_spaces(errstr[0]);
		resultLine = make_message("%s %d %s 0 N/A", reqId, retcode, esc_errstr);
		enqueue_result(resultLine);
		free(resultLine);
		free(esc_errstr);
	}

	/* Free up all arguments */
	free_args(argv);
	sem_post(&sem_total_commands);
	return;
}

void*
cmd_status_job_all(void *args)
{
	char *resultLine=NULL;
	char *str_cad=NULL;
	char *en_cad, *new_str_cad;
	int  str_cad_app, str_cad_len=0;
	char *esc_str_cad, *esc_errstr;
	char **argv = (char **)args;
	char *reqId = argv[1];
	char *selectad = argv[2]; /* May be NULL */
	classad_expr_tree selecttr = NULL;
	int jobStatus, n_jobs=0;
	FILE *fd;
	job_registry_entry *en;
	int select_ret, select_result;

	if (blah_children_count>0) check_on_children(blah_children, blah_children_count);

	/* File locking will not protect threads in the same */
	/* process. */
	pthread_mutex_lock(&blah_jr_lock);

	fd = job_registry_open(blah_jr_handle, "r");
	if (fd == NULL)
	{
	  	/* Report error opening registry. */
		esc_errstr = escape_spaces(strerror(errno));
		resultLine = make_message("%s 1 Cannot\\ open\\ BLAH\\ job\\ registry:\\ %s N/A", reqId, esc_errstr);
		free(esc_errstr);
		goto wrap_up;
	}
	if (job_registry_rdlock(blah_jr_handle, fd) < 0)
	{
	  	/* Report error locking registry. */
		esc_errstr = escape_spaces(strerror(errno));
		resultLine = make_message("%s 1 Cannot\\ lock\\ BLAH\\ job\\ registry:\\ %s N/A", reqId, esc_errstr);
		free(esc_errstr);
		goto wrap_up;
	}

	if (selectad != NULL)
	{
		selecttr = classad_parse_expr(selectad);
	}

	while ((en = job_registry_get_next(blah_jr_handle, fd)) != NULL)
	{
		en_cad = job_registry_entry_as_classad(blah_jr_handle, en);
		if (en_cad != NULL)
		{
			if (selecttr != NULL)
			{
				select_ret = classad_evaluate_boolean_expr(en_cad,selecttr,&select_result);
				if ((select_ret == C_CLASSAD_NO_ERROR && !select_result) ||
				     select_ret != C_CLASSAD_NO_ERROR)
				{
					free(en_cad);
					free(en);
					continue;
				}
			}
			str_cad_app = str_cad_len;
			str_cad_len += (strlen(en_cad)+1);
			new_str_cad = realloc(str_cad,str_cad_len);
			if (new_str_cad == NULL)
			{
				free(str_cad);
				free(en_cad);
				free(en);
				resultLine = make_message("%s 1 Out\\ of\\ memory\\ servicing\\ status_all\\ request N/A", reqId);
				goto wrap_up;
			}
			str_cad = new_str_cad;
			if (str_cad_app > 0)
			{
				strcat(str_cad,";"); 
			} else {
				str_cad[0] = '\000';
			}
			strcat(str_cad, en_cad);
			n_jobs++;
			free(en_cad);
		}
		free(en);
	}
	if (str_cad != NULL)
	{
		esc_str_cad = escape_spaces(str_cad);
		if (esc_str_cad != NULL)
			resultLine = make_message("%s 0 No\\ error [%s]", reqId, esc_str_cad);
		else resultLine = make_message("%s 1 Out\\ of\\ memory\\ servicing\\ status_all\\ request N/A", reqId);
		free(str_cad);
		if (esc_str_cad != NULL) free(esc_str_cad);
	} else {
		resultLine = make_message("%s 0 No\\ error []", reqId);
	}

wrap_up:
	if (selecttr != NULL) classad_free_tree(selecttr);
	if (fd != NULL) fclose(fd);
	pthread_mutex_unlock(&blah_jr_lock);

	/* Free up all arguments */
	free_args(argv);
	if(resultLine)
	{
		enqueue_result(resultLine);
		free (resultLine);
	}
	sem_post(&sem_total_commands);
	return;
}

int
get_status_and_old_proxy(int use_glexec, char *jobDescr, 
			char **status_argv, char **old_proxy,
			char **workernode, char **error_string)
{
	char *proxy_link;
	char *r_old_proxy=NULL;
	int readlink_res, retcod;
	classad_context status_ad[MAX_JOB_NUMBER];
	char errstr[MAX_JOB_NUMBER][ERROR_MAX_LEN];
	int jobNumber=0, jobStatus;
	char *command, *escaped_command;
	char error_buffer[ERROR_MAX_LEN];
	job_registry_split_id *spid;
	job_registry_entry *ren;
	int i;

	if (old_proxy == NULL) return(-1);
	*old_proxy = NULL;
	if (workernode == NULL) return(-1);
	*workernode = NULL;
	if (error_string != NULL) *error_string = NULL;

	/* Look up job registry first, if configured. */
	if (blah_jr_handle != NULL)
	{
		/* File locking will not protect threads in the same */
		/* process. */
		pthread_mutex_lock(&blah_jr_lock);
		if ((ren=job_registry_get(blah_jr_handle, jobDescr)) != NULL)
		{
			*old_proxy = job_registry_get_proxy(blah_jr_handle, ren);
			if (*old_proxy != NULL && ren->renew_proxy == 0)
			{
				free(ren);
				pthread_mutex_unlock(&blah_jr_lock);
				return 1; /* 'local' state */
			} 
			jobStatus = ren->status;
			*workernode = strdup(ren->wn_addr);
			free(ren);
			pthread_mutex_unlock(&blah_jr_lock);
			return jobStatus;	
		}
		pthread_mutex_unlock(&blah_jr_lock);
	}

	/* FIXMEPRREG: this entire, complex recipe to retrieve */
	/* FIXMEPRREG: the job proxy and status can be removed once */
	/* FIXMEPRREG: the job registry is used throughout. */

	spid = job_registry_split_blah_id(jobDescr);
	if (spid == NULL) return(-1); /* Error */

	if (!use_glexec)
	{
		if ((r_old_proxy = (char *)malloc(FILENAME_MAX)) == NULL)
		{
			fprintf(stderr, "Out of memory.\n");
			exit(MALLOC_ERROR);
		}
		if ((proxy_link = make_message("%s/.blah_jobproxy_dir/%s.proxy", getenv("HOME"), spid->proxy_id)) == NULL)
	 	{
			fprintf(stderr, "Out of memory.\n");
			exit(MALLOC_ERROR);
		}
		if ((readlink_res = readlink(proxy_link, r_old_proxy, FILENAME_MAX - 2)) == -1)
		{
			if (error_string != NULL)
			{
				*error_string = escape_spaces(strerror_r(errno, error_buffer, sizeof(error_buffer)));
			}
			/* Proxy link for renewal is not accessible */
			/* Try with .norenew */
			free(proxy_link);
			if ((proxy_link = make_message("%s/.blah_jobproxy_dir/%s.proxy.norenew", getenv("HOME"), spid->proxy_id)) == NULL)
	 		{
				fprintf(stderr, "Out of memory.\n");
				exit(MALLOC_ERROR);
			}
			if ((readlink_res = readlink(proxy_link, r_old_proxy, FILENAME_MAX - 2)) >= 0)
			{
				/* No need to check for job status - */
				/* Proxy has to be renewed locally */
				r_old_proxy[readlink_res] = '\000'; /* readlink does not append final NULL */
				*old_proxy = r_old_proxy;
				free(proxy_link);
				job_registry_free_split_id(spid);
				return 1; /* 'local' state */
			}
			free(proxy_link);
			free(r_old_proxy);
			job_registry_free_split_id(spid);
			return -1; /* Error */
		}
		r_old_proxy[readlink_res] = '\000'; /* readlink does not append final NULL */
		*old_proxy = r_old_proxy;
		free(proxy_link);

	}
	else
	{
		/* GLEXEC case */
		command = make_message("%s /usr/bin/readlink -n .blah_jobproxy_dir/%s.proxy", gloc, spid->proxy_id);
		if (command == NULL)
	 	{
			fprintf(stderr, "Out of memory.\n");
			exit(MALLOC_ERROR);
		}
		retcod = exe_getout(command, status_argv, &r_old_proxy);
		if (r_old_proxy == NULL || strlen(r_old_proxy) == 0 || retcod != 0)
		{
			if (r_old_proxy != NULL)
			{
				free(r_old_proxy);
				r_old_proxy = NULL;
			}

			if (error_string != NULL)
			{
				escaped_command = escape_spaces(command);
				*error_string = make_message("%s\\ returns\\ %d\\ and\\ no\\ proxy.", escaped_command, retcod);;
				free(escaped_command);
			}
			/* Proxy link for renewal is not accessible */
			/* Try with .norenew */
			free(command);
			command = make_message("%s /usr/bin/readlink -n .blah_jobproxy_dir/%s.proxy.norenew", gloc, spid->proxy_id);
			retcod = exe_getout(command, status_argv, &r_old_proxy);
			if (r_old_proxy != NULL && strlen(r_old_proxy) > 0 && retcod == 0)
			{
				/* No need to check for job status - */
				/* Proxy has to be renewed locally */
				*old_proxy = r_old_proxy;
				free(command);
				job_registry_free_split_id(spid);
				return 1; /* 'local' state */
			}
			if (r_old_proxy != NULL) free(r_old_proxy);
			free(command);
			job_registry_free_split_id(spid);
			return(-1);
		}
		*old_proxy = r_old_proxy;
		free(command);
	}

	job_registry_free_split_id(spid);

	/* If we have a proxy link, and proxy renewal on worker nodes */
	/* was disabled, we need to deal with the proxy locally. */
	/* We don't need to spend time checking on job status. */
	if (disable_wn_proxy_renewal) return 1; /* 'Local' renewal only. */

	/* If we reach here we have a proxy *and* we have */
	/* to check on the job status */
	retcod = get_status(jobDescr, status_ad, status_argv, errstr, 1, &jobNumber);

	if (jobNumber > 0 && (!strcmp(errstr[0], "No Error")))
	{
		classad_get_int_attribute(status_ad[0], "JobStatus", &jobStatus);
		retcod = classad_get_dstring_attribute(status_ad[0], "WorkerNode", workernode);
		for (i=0; i<jobNumber; i++) if (status_ad[i]) classad_free(status_ad[i]);
		return jobStatus;
	}
	if (error_string != NULL)
	{
		*error_string = escape_spaces(errstr[0]);	
	}
	if (jobNumber > 0) 
	{
		for (i=0; i<jobNumber; i++) if (status_ad[i]) classad_free(status_ad[i]);
	}
	return -1;
}

#define CMD_RENEW_PROXY_ARGS 3
void *
cmd_renew_proxy(void *args)
{
	char *resultLine;
	char **argv = (char **)args;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	char *proxyFileName = argv[3];
	char *workernode = NULL;
	char *command = NULL;
	char *old_proxy = NULL;
	char *dummy_cmd_out = NULL;
	
	int i, jobStatus, retcod, count;
	char *cmd_out;
	char *error_string = NULL;
	char *proxyFileNameNew = NULL;
	int use_glexec;

	use_glexec = (argv[CMD_RENEW_PROXY_ARGS + 1] != NULL);

	if (blah_children_count>0) check_on_children(blah_children, blah_children_count);

	if ((jobStatus=get_status_and_old_proxy(use_glexec, jobDescr, argv + CMD_RENEW_PROXY_ARGS + 1, &old_proxy, &workernode, &error_string)) < 0)
	{
		resultLine = make_message("%s 1 Cannot\\ locate\\ old\\ proxy:\\ %s", reqId, error_string);
		if (error_string != NULL) free(error_string);
		if (old_proxy != NULL) free(old_proxy);
		if (workernode != NULL) free(workernode);
	}
	else
	{
		if (error_string != NULL) free(error_string);
		switch(jobStatus)
		{
			case 1: /* job queued: copy the proxy locally */
				/* FIXME: add all the controls */
				if (!use_glexec)
				{
					/* Not in GLEXEC mode */
					limit_proxy(proxyFileName, old_proxy);
					resultLine = make_message("%s 0 Proxy\\ renewed", reqId);
				}
				else
				{
					/* GLEXEC mode */

					/* add the target proxy */
					for(count = CMD_RENEW_PROXY_ARGS + 2; argv[count]; count++);
					argv = (char **)realloc(argv, sizeof(char *) * (count + 2));
					argv[count] = make_message("GLEXEC_TARGET_PROXY=%s", old_proxy);
					argv[count + 1] = NULL;
					/* FIXME: should not execute anything, just create the new copy of the proxy (not yet supported by glexec) */
					command = make_message("%s /bin/pwd", gloc);
					retcod = exe_getout(command, argv + CMD_RENEW_PROXY_ARGS + 1, &dummy_cmd_out);
					if (retcod == 0)
					{
						resultLine = make_message("%s 0 Proxy\\ renewed\\ via\\ glexec", reqId);
					} else {
						resultLine = make_message("%s 1 glexec\\ failed\\ (exitcode==%d)", reqId, retcod);
					}
					free(command);
				}
				if (dummy_cmd_out != NULL) free(dummy_cmd_out);
				break;

			case 2: /* job running: send the proxy to remote host */
				if (workernode != NULL && strcmp(workernode, ""))
				{
					/* Add the worker node argument to argv and invoke cmd_send_proxy_to_worker_node */
					for(count = CMD_RENEW_PROXY_ARGS + 1; argv[count]; count++);
					argv = (char **)realloc(argv, sizeof(char *) * (count + 2));
					if (argv != NULL)
					{
						/* Make room for the workernode argument at i==CMD_RENEW_PROXY_ARGS+1. */
						argv[count+1] = 0;
						for(i = count; i > (CMD_RENEW_PROXY_ARGS+1); i--) argv[i] = argv[i-1];
						/* workernode will be freed inside cmd_send_proxy_to_worker_node */
						/* also the semaphore will be released there */
						argv[CMD_RENEW_PROXY_ARGS+1] = workernode;
						cmd_send_proxy_to_worker_node((void *)argv);
						if (old_proxy != NULL) free(old_proxy);
						return;
					}
					else
					{
						fprintf(stderr, "blahpd: out of memory! Exiting...\n");
						exit(MALLOC_ERROR);
					}
				}
				else
				{
					resultLine = make_message("%s 1 Cannot\\ retrieve\\ executing\\ host", reqId);
				}
				break;
			case 3: /* job deleted */
				/* no need to refresh the proxy */
				resultLine = make_message("%s 0 No\\ proxy\\ to\\ renew\\ -\\ Job\\ was\\ deleted", reqId);
				break;
			case 4: /* job completed */
				/* no need to refresh the proxy */
				resultLine = make_message("%s 0 No\\ proxy\\ to\\ renew\\ -\\ Job\\ completed", reqId);
				break;
			case 5: /* job hold */
				/* FIXME not yet supported */
				resultLine = make_message("%s 0 No\\ support\\ for\\ renewing\\ held\\ jobs\\ yet", reqId);
				break;
			default:
				resultLine = make_message("%s 1 Job\\ is\\ in\\ an\\ unknown\\ status\\ (%d)", reqId, jobStatus);
		}
		if (old_proxy != NULL) free(old_proxy);
		if (workernode != NULL) free(workernode);
	}
		
	if (resultLine)
	{
		enqueue_result(resultLine);
		free(resultLine);
	}
	else
	{
		fprintf(stderr, "blahpd: out of memory! Exiting...\n");
		exit(MALLOC_ERROR);
	}
	
	/* Free up all arguments */
	free_args(argv);
	sem_post(&sem_total_commands);
	return;
}

#define CMD_SEND_PROXY_TO_WORKER_NODE_ARGS 4
void *
cmd_send_proxy_to_worker_node(void *args)
{
	char *resultLine;
	char **argv = (char **)args;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	char *proxyFileName = argv[3];
	char *workernode = argv[4];
	char *command = NULL;
	int count,retcod;
	
	char *cmd_out;
	char *error_string = NULL;
	char *proxyFileNameNew = NULL;

	char *delegate_switch;

	if (workernode != NULL && strcmp(workernode, ""))
	{
		if(argv[CMD_SEND_PROXY_TO_WORKER_NODE_ARGS + 1] == NULL)
		{
			proxyFileNameNew = make_message("%s.lmt", proxyFileName);
			limit_proxy(proxyFileName, proxyFileNameNew);
		}
		else
			proxyFileNameNew = strdup(argv[CMD_SEND_PROXY_TO_WORKER_NODE_ARGS + GLEXEC_SOURCE_PROXY_IDX + 1] + 
			                           strlen(glexec_env_name[GLEXEC_SOURCE_PROXY_IDX]) + 1);

		/* Add the globus library path */
		for(count = CMD_SEND_PROXY_TO_WORKER_NODE_ARGS + 1; argv[count]; count++);
		argv = (char **)realloc(argv, sizeof(char *) * (count + 2));
		if (argv == NULL)
		{
			fprintf(stderr, "blahpd: out of memory! Exiting...\n");
			exit(MALLOC_ERROR);
		}
		argv[count] = make_message("LD_LIBRARY_PATH=%s/lib",
		                           getenv("GLOBUS_LOCATION") ? getenv("GLOBUS_LOCATION") : "/opt/globus");
		argv[count + 1] = NULL;

		delegate_switch = "";
		if (config_test_boolean(config_get("blah_delegate_renewed_proxies",blah_config_handle)))
			delegate_switch = "delegate_proxy";

		command = make_message("%s/BPRclient %s %s %s %s",
		                       blah_script_location, proxyFileNameNew, jobDescr, workernode, delegate_switch); 
		free(proxyFileNameNew);

		retcod = exe_getout(command, argv + CMD_RENEW_PROXY_ARGS + 1, &cmd_out);
		if (cmd_out)
		{
			error_string = escape_spaces(cmd_out);
			free(cmd_out);
		}
		else
			error_string = strdup("Cannot\\ execute\\ BPRclient");

		resultLine = make_message("%s %d %s", reqId, retcod, error_string);
		free(error_string);
		free(command);
	}
	else
	{
		resultLine = make_message("%s 1 Worker\\ node\\ empty.", reqId);
	}
		
	if (resultLine)
	{
		enqueue_result(resultLine);
		free(resultLine);
	}
	else
	{
		fprintf(stderr, "blahpd: out of memory! Exiting...\n");
		exit(MALLOC_ERROR);
	}
	
	/* Free up all arguments */
	free_args(argv);
	sem_post(&sem_total_commands);
	return;
}

void
hold_res_exec(char* jobdescr, char* reqId, char* action, int status, char **environment )
{
	int retcod;
	char *cmd_out, *cmd_err;
	char *escpd_cmd_out, *escpd_cmd_err;
	char *command;
	char *resultLine = NULL;
	job_registry_split_id *spid;
	char *error_string;

	/* Split <lrms> and actual job Id */
	if((spid = job_registry_split_blah_id(jobdescr)) == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 2 Malformed\\ jobId\\ %s\\ or\\ out\\ of\\ memory", reqId, jobdescr);
		goto cleanup_argv;
	}

	if(*environment)
	{
		if(!strcmp(action,"hold"))
		{
		        command = make_message("%s %s/%s_%s.sh %s %d", gloc, blah_script_location, spid->lrms, action, spid->script_id, status);
		}else
		{
		        command = make_message("%s %s/%s_%s.sh %s", gloc, blah_script_location, spid->lrms, action, spid->script_id);
		}
	}else
	{
		if(!strcmp(action,"hold"))
		{
		        command = make_message("%s/%s_%s.sh %s %d", blah_script_location, spid->lrms, action, spid->script_id, status);
		}else
		{
		        command = make_message("%s/%s_%s.sh %s", blah_script_location, spid->lrms, action, spid->script_id);
		}
	}

	if (command == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Cannot\\ allocate\\ memory\\ for\\ the\\ command\\ string", reqId);
		goto cleanup_lrms;
	}

	/* Execute the command */
	retcod = exe_getouterr(command, environment, &cmd_out, &cmd_err);
	if(cmd_out == NULL)
	{
		resultLine = make_message("%s 1 Cannot\\ execute\\ %s\\ script", reqId, command);
		goto cleanup_command;
	}
	if(retcod)
	{
		escpd_cmd_out = escape_spaces(cmd_out);
		if (escpd_cmd_out == NULL) escpd_cmd_out = blah_omem_msg;
		escpd_cmd_err = escape_spaces(cmd_err);
		if (escpd_cmd_err == NULL) escpd_cmd_err = blah_omem_msg;
		resultLine = make_message("%s %d Job\\ %s:\\ %s\\ not\\ supported\\ by\\ %s\\ (stdout:%s)\\ (stderr:%s)", reqId, retcod, statusstring[status - 1], action, spid->lrms, escpd_cmd_out, escpd_cmd_err);
		if (escpd_cmd_out != blah_omem_msg) free(escpd_cmd_out);
		if (escpd_cmd_err != blah_omem_msg) free(escpd_cmd_err);
	}else
		resultLine = make_message("%s %d No\\ error", reqId, retcod);

	if (cmd_out != NULL) free(cmd_out);
	if (cmd_err != NULL) free(cmd_err);

	/* Free up all arguments and exit (exit point in case of error is the label
	   pointing to last successfully allocated variable) */
cleanup_command:
	free(command);
cleanup_lrms:
	job_registry_free_split_id(spid);
cleanup_argv:
	if(resultLine)
	{
		enqueue_result(resultLine);
		free (resultLine);
	}
	else
	{
		fprintf(stderr, "blahpd: out of memory! Exiting...\n");
		exit(MALLOC_ERROR);
	}
	
	/* argv is cleared in the calling function */
	return;
}

#define HOLD_RESUME_ARGS 2
void
hold_resume(void* args, int action )
{
	classad_context status_ad[MAX_JOB_NUMBER];
	char **argv = (char **)args;
	char errstr[MAX_JOB_NUMBER][ERROR_MAX_LEN];
	char *resultLine = NULL;
	int jobStatus, retcode;
	char *reqId = argv[1];
	char jobdescr[MAX_JOB_NUMBER][JOBID_MAX_LEN];
	int i,job_number;
	char *dummyargv = argv[2];
	char *tmpjobdescr=NULL;
	char *pointer;

	/* job status check */
	retcode = get_status(dummyargv, status_ad, argv + HOLD_RESUME_ARGS + 1, errstr, 0, &job_number);
	/* if multiple jobs are present their id must be extracted from argv[2] */
	i=0;
#if 1
	tmpjobdescr = strtok_r(dummyargv," ", &pointer);
	strncpy(jobdescr[0], tmpjobdescr, JOBID_MAX_LEN);
	if(job_number>1)
	{
		for (i=1; i<job_number; i++)
		{
		        tmpjobdescr = strtok_r(NULL," ", &pointer);
		        strncpy(jobdescr[i], tmpjobdescr, JOBID_MAX_LEN);
		}
	}
#else
	strncpy(jobdescr[0], dummyargv, JOBID_MAX_LEN);
#endif
	if (!retcode)
	{
		for (i=0; i<job_number; i++)
		{
		        if(job_number>1)
		        {
		                reqId = make_message("%s.%d",argv[1],i);
		        }else
				reqId = strdup(argv[1]);
		        if(classad_get_int_attribute(status_ad[i], "JobStatus", &jobStatus) == C_CLASSAD_NO_ERROR)
		        {
		                switch(jobStatus)
		                {
		                        case 1:/* IDLE */
		                                if(action == HOLD_JOB)
		                                {
		                                        hold_res_exec(jobdescr[i], reqId, "hold", 1, argv + HOLD_RESUME_ARGS + 1);
		                                }
		                                else
		                                if (resultLine = make_message("%s 1 Job\\ Idle\\ jobId\\ %s", reqId,jobdescr[i]))
		                                {
		                                        enqueue_result(resultLine);
		                                        free(resultLine);
		                                }
		                        break;
		                        case 2:/* RUNNING */
		                                if(action == HOLD_JOB)
		                                {
		                                        hold_res_exec(jobdescr[i], reqId, "hold", 2, argv + HOLD_RESUME_ARGS + 1);
		                                }else
		                                if (resultLine = make_message("%s 1 \\ Job\\ Running\\ jobId\\ %s", reqId, jobdescr[i]))
		                                {
		                                        enqueue_result(resultLine);
		                                        free(resultLine);
		                                }
		                        break;
		                        case 3:/* REMOVED */
		                                if (resultLine = make_message("%s 1 Job\\ Removed\\ jobId\\ %s", reqId, jobdescr[i]))
		                                {
		                                        enqueue_result(resultLine);
		                                        free(resultLine);
		                                }
		                        break;
		                        case 4:/* COMPLETED */
		                                if (resultLine = make_message("%s 1 Job\\ Completed\\ jobId\\ %s", reqId, jobdescr[i]))
		                                {
		                                        enqueue_result(resultLine);
		                                        free(resultLine);
		                                }
		                        break;
		                        case 5:/* HELD */
		                                if(action == RESUME_JOB)
		                                        hold_res_exec(jobdescr[i], reqId, "resume", 5, argv + HOLD_RESUME_ARGS + 1);
		                                else
		                                if (resultLine = make_message("%s 0 Job\\ Held\\ jobId\\ %s", reqId, jobdescr[i]))
		                                {
		                                        enqueue_result(resultLine);
		                                        free(resultLine);
		                                }
		                        break;
		                }
		        }else
		        if (resultLine = make_message("%s 1 %s", reqId, errstr[i]))
		        {
		                enqueue_result(resultLine);
		                free(resultLine);
		        }
			if(reqId) {free(reqId);reqId=NULL;}
		}
	}else
	{
		resultLine = make_message("%s %d %s", reqId, retcode, errstr[0]);
		enqueue_result(resultLine);
		free(resultLine);
	}
	if (dummyargv) free(dummyargv);
	if (reqId) free(reqId);
	return;
}

void *
cmd_hold_job(void* args)
{
	hold_resume(args,HOLD_JOB);
	sem_post(&sem_total_commands);
	return;
}

void *
cmd_resume_job(void* args)
{
	hold_resume(args,RESUME_JOB);
	sem_post(&sem_total_commands);
	return;
}

void *
cmd_get_hostport(void *args)
{
	char **argv = (char **)args;
	char *reqId = argv[1];
	char *cmd_out;
	char *command;
	char *resultLine;
	int  retcode;
	int i;

	if (blah_children_count>0) check_on_children(blah_children, blah_children_count);

	if (lrms_counter)
	{
		resultLine = make_message("%s 0 ", reqId);
		for(i = 0; i < lrms_counter; i++)
		{        
			command = make_message("%s/%s_status.sh -n", blah_script_location, lrmslist[i]);
			retcode = exe_getout(command, NULL, &cmd_out);
			free(command);
			if (retcode || !cmd_out)
			{
				free(resultLine);
				resultLine = make_message("%s 1 Unable\\ to\\ retrieve\\ the\\ port\\ (exit\\ code\\ =\\ %d) ", reqId, retcode);
				if (cmd_out) free (cmd_out);
				break;
			}
			if (cmd_out[strlen(cmd_out) - 1] == '\n') cmd_out[strlen(cmd_out) - 1] = '\0';
			resultLine = make_message("%s%s/%s ", resultLine, lrmslist[i], strlen(cmd_out) ? cmd_out : "Error\\ reading\\ host:port");
			free(cmd_out);
		}
		/* remove the trailing space */
		resultLine[strlen(resultLine) - 1] = '\0';
	}
	else
	{
		resultLine = make_message("%s 1 No\\ LRMS\\ found", reqId);
	}

	enqueue_result(resultLine);
	free(resultLine);
	free_args(argv);
	sem_post(&sem_total_commands);
	return ;
}

/* Utility functions
 * */

char 
*escape_spaces(const char *str)
{
	char *buffer;
	char cur;
	int i, j;

	if ((buffer = (char *) malloc (strlen(str) * 2 + 1)) == NULL)
	{
		fprintf(stderr, "Out of memory.\n");
		exit(MALLOC_ERROR);
	}

	for (i = 0, j = 0; i <= strlen(str); i++, j++)
	{
		cur = str[i];
		if (cur == '\r') cur = '-';
		else if (cur == '\n') cur = '-';
		else if (cur == '\t') cur = ' ';

		if (cur == ' ') buffer[j++] = '\\';
		buffer[j] = cur;
	}
	/* FIXME 24-11-06 what is this realloc for??????? */
	/* realloc(buffer, strlen(buffer) + 1); */
	return(buffer);
}

char*
get_command(int s)
{
	static char *cmd_queue = NULL;
	static char *next_cmd;
	static char *queue_end;
	char *message = NULL;
	char *tmp_realloc;
	int allocated_size = 0;
	char buffer[2047];
	int read_chars = 0; 
	int recv_chars, i;
	int done = FALSE;
	char last_char;

	/* if the queue is empty, read from the socket */
	if (!cmd_queue)
	{
		while (!done)
		{
			if ((recv_chars = read(s, buffer, sizeof(buffer))) > 0)
			{
				if ((read_chars + recv_chars) > allocated_size)
				{
					allocated_size += sizeof(buffer) + 1;
					tmp_realloc = (char *) realloc (message, allocated_size);
					if (tmp_realloc == NULL)
					{
						allocated_size = 0;
						perror("Error allocating buffer for incoming message");
						close(s);
						if (message) free(message);
						exit(MALLOC_ERROR);
					}
					else
						message = tmp_realloc;
				}
				memcpy(&message[read_chars], buffer, recv_chars);
				read_chars += recv_chars;
				message[read_chars] = '\000';
			} else {
				/* Error or EOF */
				break;
			}
			if (message != NULL) {
				/* Require LF terminated messages */
				last_char = message[read_chars -1];
				if (last_char == '\n') break;
			}
		}
	
		if (recv_chars <= 0)
		{
			return(NULL);
		}
		else if (read_chars > 0)
		{
			/* return(message); */
			cmd_queue = strdup(message);
			next_cmd = cmd_queue;
			queue_end = cmd_queue + read_chars;
			free(message);
		}
	}

	/* save the pointer to current command */
	message = next_cmd;

	/* search for end of current command */
	while(next_cmd <= queue_end)
	{
		if (*next_cmd == '\n' || *next_cmd == '\r' || *next_cmd == '\000') break;
		next_cmd++;
	}

	/* mark end of command */
	*next_cmd = '\000';
	
	/* make a copy of the command to be returned */
	message = strdup(message);
	if (message == NULL)
	{
		fprintf(stderr, "Out of memory.\n");
		exit(MALLOC_ERROR);
	}

	/* search for beginning of next command */
	next_cmd++;
	while(next_cmd <= queue_end)
	{
		if ((*next_cmd != '\n' && *next_cmd != '\r') || *next_cmd == '\000') break;
		next_cmd++;
	}
	
	/* if we reached end of queue free all */
	if (next_cmd >= queue_end)
	{
		free(cmd_queue);
		cmd_queue = NULL;
		next_cmd = NULL;
	}
	
	return(message);
}

int
enqueue_result(char *res)
{
	if (push_result(res))
	{
		pthread_mutex_lock(&send_lock);
		write(server_socket, "R\r\n", 3);
		pthread_mutex_unlock(&send_lock);
	}
}

int
set_cmd_string_option(char **command, classad_context cad, const char *attribute, const char *option, const int quote_style)
{
	char *argument;
	char *to_append = NULL;
	char *new_command;
	int result;
	
	if ((result = classad_get_dstring_attribute(cad, attribute, &argument)) == C_CLASSAD_NO_ERROR)
	{
		if (strlen(argument) > 0)
		{
			if ((to_append = make_message(opt_format[quote_style], option, argument)) == NULL)
				result = C_CLASSAD_OUT_OF_MEMORY;
			free (argument);
		}
		else
			result = C_CLASSAD_VALUE_NOT_FOUND;
	}

	if (result == C_CLASSAD_NO_ERROR)
		if (new_command = (char *) realloc (*command, strlen(*command) + strlen(to_append) + 1))
		{
			strcat(new_command, to_append);
			*command = new_command;
		}
		else
			result = C_CLASSAD_OUT_OF_MEMORY;

	if (to_append) free (to_append);
	return(result);
}

int
set_cmd_int_option(char **command, classad_context cad, const char *attribute, const char *option, const int quote_style)
{
	int argument;
	char *to_append = NULL;
	char *new_command;
	int result;
	
	if ((result = classad_get_int_attribute(cad, attribute, &argument)) == C_CLASSAD_NO_ERROR)
	{
		if ((to_append = make_message(opt_format[quote_style], option, argument)) == NULL)
		{
			result = C_CLASSAD_OUT_OF_MEMORY;
		}
	}

	if (result == C_CLASSAD_NO_ERROR)
		if (new_command = (char *) realloc (*command, strlen(*command) + strlen(to_append) + 1))
		{
			strcat(new_command, to_append);
			*command = new_command;
		}
		else
			result = C_CLASSAD_OUT_OF_MEMORY;

	if (to_append) free (to_append);
	return(result);
}

int
set_cmd_bool_option(char **command, classad_context cad, const char *attribute, const char *option, const int quote_style)
{
	const char *str_yes = "yes";
	const char *str_no  = "no";
	int attr_value;
	char *argument;
	char *to_append = NULL;
	char *new_command;
	int result;
	
	if ((result = classad_get_bool_attribute(cad, attribute, &attr_value)) == C_CLASSAD_NO_ERROR)
	{
		argument = (char *)(attr_value ? str_yes : str_no);
		if ((to_append = make_message(opt_format[quote_style], option, argument)) == NULL)
			result = C_CLASSAD_OUT_OF_MEMORY;
	}

	if (result == C_CLASSAD_NO_ERROR)
		if (new_command = (char *) realloc (*command, strlen(*command) + strlen(to_append) + 1))
		{
			strcat(new_command, to_append);
			*command = new_command;
		}
		else
			result = C_CLASSAD_OUT_OF_MEMORY;

	if (to_append) free (to_append);
	return(result);
}


int
set_cmd_list_option(char **command, classad_context cad, const char *attribute, const char *option)
{
	char **list_cont;
	char **str_ptr;
	char *to_append = NULL;
	char *reallocated;
	int result;
	
	if ((result = classad_get_string_list_attribute(cad, attribute, &list_cont)) == C_CLASSAD_NO_ERROR)
	{
		if (to_append = strdup(option))
		{
			for (str_ptr = list_cont; (*str_ptr) != NULL; str_ptr++)
			{
				if (reallocated = (char *) realloc (to_append, strlen(*str_ptr) + strlen(to_append) + 2))
				{
					to_append = reallocated;
					strcat(to_append, " ");
					strcat(to_append, *str_ptr);
				}
				else
				{
					result = C_CLASSAD_OUT_OF_MEMORY;
					break;
				}					
			}
		}
		else /* strdup failed */
			result = C_CLASSAD_OUT_OF_MEMORY;

		classad_free_string_list(list_cont);
	}

	if (result == C_CLASSAD_NO_ERROR)
		if (reallocated = (char *) realloc (*command, strlen(*command) + strlen(to_append) + 1))
		{
			strcat(reallocated, to_append);
			*command = reallocated;
		}

	if (to_append) free (to_append);
	return(result);
}

char *
make_message(const char *fmt, ...)
{
	int n;
	char *result = NULL;
	va_list ap;

	va_start(ap, fmt);
	n = vsnprintf(NULL, 0, fmt, ap) + 1;
	va_end(ap);

	result = (char *) malloc (n);
	if (result)
	{
		va_start(ap, fmt);
		vsnprintf(result, n, fmt, ap);
		va_end(ap);
	}

	return(result);
}

int
limit_proxy(char* proxy_name, char *limited_proxy_name)
{
	char *timeleftcommand;
	int seconds_left, hours_left, minutes_left;
	char *limcommand;
	char *cmd_out;
	int res;
	char* globuslocation;
	char *limit_command_output;
	int tmpfd;

	globuslocation = (getenv("GLOBUS_LOCATION") ? getenv("GLOBUS_LOCATION") : "/opt/globus");
	timeleftcommand = make_message("%s/bin/grid-proxy-info -timeleft -file %s",
	                          globuslocation, proxy_name);
	res = exe_getout(timeleftcommand, NULL, &cmd_out);
	free(timeleftcommand);
	if (!cmd_out) return -1;
	else {
		seconds_left = atoi(cmd_out);
		free(cmd_out);
	}

	limit_command_output = make_message("%s_XXXXXX", limited_proxy_name);
	if (limit_command_output != NULL)
	{
		tmpfd = mkstemp(limit_command_output);
		if (tmpfd < 0)
		{
			/* Fall back to direct file creation - it may work */
			free(limit_command_output);
			limit_command_output = limited_proxy_name;
		}
		else
		{
			close(tmpfd);
			/* Make sure file gets created by grid-proxy-init */
			unlink(limit_command_output);
		}
	}
        
	if (seconds_left <= 0) {
		/* Something's wrong with the current proxy - use defaults */
		limcommand = make_message("%s/bin/grid-proxy-init -old -limited -cert %s -key %s -out %s",
	                          globuslocation, proxy_name, proxy_name, limit_command_output);
	} else {
		hours_left = (int)(seconds_left/3600);
		minutes_left = (int)((seconds_left%3600)/60) + 1;
		limcommand = make_message("%s/bin/grid-proxy-init -old -limited -valid %d:%d -cert %s -key %s -out %s",
	                          globuslocation, hours_left, minutes_left, proxy_name, proxy_name, limit_command_output);
	}
	res = exe_getout(limcommand, NULL, &cmd_out);
	free(limcommand);
	if (!cmd_out) 
	{
		if (limit_command_output != limited_proxy_name)
			free(limit_command_output);
		return -1;
	}
	else free(cmd_out);

	/* If exitcode != 0 there may be a problem due to a warning by grid-proxy-init but */
	/* the call may have been successful. We just check the temporary proxy  */
	if (res)
	{
		limcommand = make_message("%s/bin/grid-proxy-info -f %s", globuslocation, limit_command_output);
		res = exe_getout(limcommand, NULL, &cmd_out);
		free(limcommand);
		if (res != 0 || !cmd_out) 
		{
			if (limit_command_output != limited_proxy_name)
				free(limit_command_output);
			return -1;
		}
		else free(cmd_out);
	}
	if (limit_command_output != limited_proxy_name)
	{
		/* Rotate limited proxy in place via atomic rename */
		res = rename(limit_command_output, limited_proxy_name);
		free(limit_command_output);
	}
	return res;
}


int  logAccInfo(char* jobId, char* server_lrms, classad_context cad, char* fqan, char* userDN, char** environment)
{
	int i=0, rc=0, cs=0, result=0, fd = -1, count = 0, slen = 0, slen2 = 0;
	FILE *cmd_out=NULL;
	FILE *conf_file=NULL;
	char *log_line;
	char *proxname=NULL;
	char *gridjobid=NULL;
	char *ce_id=NULL;
	char *ce_idtmp=NULL;
	char *login=NULL;
	char *temp_str=NULL;
	char date_str[MAX_TEMP_ARRAY_SIZE], jobid_trunc[MAX_TEMP_ARRAY_SIZE];
	time_t tt;
	struct tm *t_m=NULL;
	char *glite_loc=NULL;
	char host_name[MAX_TEMP_ARRAY_SIZE];
	char *jobid=NULL;
	char *lrms_jobid=NULL;
	int id;
	char bs[4];
	char *queue=NULL;
	char *esc_userDN=NULL;
	char *uid;
	memset(jobid_trunc,0,MAX_TEMP_ARRAY_SIZE);

	/* Get values from environment and compose the logfile pathname */
	if ((glite_loc = getenv("GLITE_LOCATION")) == NULL)
	{
		glite_loc = DEFAULT_GLITE_LOCATION;
	}
	/* Submission time */
	time(&tt);
	t_m = gmtime(&tt);
	/* sprintf(date_str,"%04d-%02d-%02d\\\ %02d:%02d:%02d", 1900+t_m->tm_year, t_m->tm_mon+1, t_m->tm_mday, t_m->tm_hour, t_m->tm_min, t_m->tm_sec);*/
	sprintf(date_str,"%04d-%02d-%02d\\ %02d:%02d:%02d", 1900+t_m->tm_year, t_m->tm_mon+1, t_m->tm_mday, t_m->tm_hour, t_m->tm_min, t_m->tm_sec);

	/* These data must be logged in the log file:
	 "timestamp=<submission time to LRMS>" "userDN=<user's DN>" "userFQAN=<user's FQAN>"
	 "ceID=<CE ID>" "jobID=<grid job ID>" "lrmsID=<LRMS job ID>"
	*/
	/* grid jobID  : if we are here we suppose that the edg_jobid is present, if not we log an empty string*/
	if(*environment)
	{
		classad_get_dstring_attribute(cad, "edg_jobid", &gridjobid);
		if(gridjobid==NULL) classad_get_dstring_attribute(cad, "uniquejobid", &gridjobid);
	}else
		classad_get_dstring_attribute(cad, "edg_jobid", &gridjobid);
	if(!gridjobid) gridjobid=make_message("");

	/* job ID */
	/*
	strncpy(jobid_trunc, &jobId[JOBID_PREFIX_LEN], strlen(jobId) - JOBID_PREFIX_LEN);
	jobid_trunc[strlen(jobId) - JOBID_PREFIX_LEN]=0;
	*/
	strncpy(jobid_trunc, jobId, sizeof(jobid_trunc));

	/* lrmsID : if hostname.domain is missing it must be added  !!!!! */
	gethostname(host_name, MAX_TEMP_ARRAY_SIZE);
	jobid = basename(jobid_trunc);
	if(strlen(jobid) <= strlen(host_name))
	{
		/*add hostname*/
		lrms_jobid=make_message("%s.%s",jobid,host_name);	

	}else
	if(strcmp(host_name,&jobid[strlen(jobid) - strlen(host_name)]))
	{
		/*add hostname*/
		lrms_jobid=make_message("%s.%s",jobid,host_name);

	}else lrms_jobid=strdup(jobid);

	/* Ce ID */
	memcpy(bs,jobid_trunc,3);
	bs[3]=0;
	classad_get_dstring_attribute(cad, "CeID", &ce_id);
	if(!ce_id) 
	{
		classad_get_dstring_attribute(cad, "Queue", &queue);
		if(queue)
		{
			ce_id=make_message("%s:2119/blah-%s-%s",host_name,bs,queue);
			free(queue);
		}else
			ce_id=make_message("%s:2119/blah-%s-",host_name,bs);
	}else
	{
		classad_get_dstring_attribute(cad, "Queue", &queue);
		if(queue&&(strncmp(&ce_id[strlen(ce_id) - strlen(queue)],queue,strlen(queue))))
		{ 
			ce_idtmp=make_message("%s-%s",ce_id,queue);
			free(ce_id);
			ce_id=ce_idtmp;
		}
		if (queue) free(queue);
	}
	if(*environment)
	{
	 	/* need to fork and glexec an id command to obtain real user */
		temp_str=make_message("%s /usr/bin/id -u",gloc);
		if (exe_getout(temp_str, environment, &uid) != 0)
			return 1;
		free(temp_str);
		uid[strlen(uid)-1]=0;	
	}else
		uid = make_message("%d",getuid());
	/* log line with in addiction unixuser */
	esc_userDN=escape_spaces(userDN);
	/* log_line=make_message("%s/BDlogger %s \\\"timestamp=%s\\\"\\\ \\\"userDN=%s\\\"\\\ %s\\\"ceID=%s\\\"\\\ \\\"jobID=%s\\\"\\\ \\\"lrmsID=%s\\\"\\\ \\\"localUser=%s\\\"", blah_script_location, blah_config_handle->config_path, date_str, esc_userDN, fqan, ce_id, gridjobid, lrms_jobid, uid); */
	log_line=make_message("%s/BDlogger %s \\\"timestamp=%s\\\"\\ \\\"userDN=%s\\\"\\ %s\\\"ceID=%s\\\"\\ \\\"jobID=%s\\\"\\ \\\"lrmsID=%s\\\"\\ \\\"localUser=%s\\\"", blah_script_location, blah_config_handle->config_path, date_str, esc_userDN, fqan, ce_id, gridjobid, lrms_jobid, uid);
	system(log_line);
	if(gridjobid) free(gridjobid);
	free(uid);
	free(log_line);
	free(esc_userDN);
	if (ce_id) free(ce_id);
	memset(fqan,0,MAX_TEMP_ARRAY_SIZE);
	free(lrms_jobid);
	return 0;
}

int getProxyInfo(char* proxname, char* fqan, char* userDN)
{
	char temp_str[MAX_TEMP_ARRAY_SIZE];
	FILE *cmd_out=NULL;
	int  result=0;
	int  slen=0;
	int  count=0;
	char fqanlong[MAX_TEMP_ARRAY_SIZE];

	sprintf(temp_str, "%s/voms-proxy-info -file %s -subject", blah_script_location, proxname);
	BLAHDBG("DEBUG: getProxyInfo: executing %s\n", temp_str);
	if ((cmd_out = mtsafe_popen(temp_str, "r")) == NULL)
		return 1;
	fgets(fqanlong, MAX_TEMP_ARRAY_SIZE, cmd_out);
	/* Trim trailing newline, if any */
	slen = strlen(fqanlong);
	if (fqanlong[slen-1] == '\n') 
	{
		fqanlong[slen-1] = '\000';
		slen--;
	}

	result = mtsafe_pclose(cmd_out);
	/* example:
	   subject= /C=IT/O=INFN/OU=Personal Certificate/L=Milano/CN=Giuseppe Fiorentino/Email=giuseppe.fiorentino@mi.infn.it/CN=proxy
	   CN=proxy, CN=limited proxy must be removed from the 
           tail of the string
	*/
	while(1)
	{
		if (!strncmp(&fqanlong[slen - 9],"/CN=proxy",9))
		{
		      memset(&fqanlong[slen - 9],0,9);
		      slen -=9;
		}else
		if (!strncmp(&fqanlong[slen - 17],"/CN=limited proxy",17))
		{
		      memset(&fqanlong[slen - 17],0,17);
		      slen -=17;
		}else
		          break;
	}
	strcpy(userDN,fqanlong);
	/* user'sFQAN detection */
	fqanlong[0]=0;
	memset(fqanlong,0,MAX_TEMP_ARRAY_SIZE);
	fqan[0]=0;
	/* command : voms-proxy-info -file proxname -fqan  */
	memset(temp_str,0,MAX_TEMP_ARRAY_SIZE);
	sprintf(temp_str,"%s/voms-proxy-info -file %s -fqan", blah_script_location, proxname);
	BLAHDBG("DEBUG: getProxyInfo: executing %s\n", temp_str);
	if ((cmd_out=mtsafe_popen(temp_str, "r")) == NULL)
		return 1;
	while(fgets(fqanlong, MAX_TEMP_ARRAY_SIZE, cmd_out))
	{
		strcat(fqan,"\\\"userFQAN=");
		strcat(fqan,fqanlong);
		memset(fqanlong,0,MAX_TEMP_ARRAY_SIZE);
		if(fqan[strlen(fqan)-1]=='\n') fqan[strlen(fqan)-1] = 0;
		/* strcat(fqan,"\\\"\\\ "); */
		strcat(fqan,"\\\"\\ ");
	}
	/* if (!strcmp(fqan,"")) sprintf(fqan,"\\\"userFQAN=\\\"\\\ "); */
	if (!strcmp(fqan,"")) sprintf(fqan,"\\\"userFQAN=\\\"\\ ");
	result = mtsafe_pclose(cmd_out);
	BLAHDBG("DEBUG: getProxyInfo returns: userDN:%s  ", userDN);
	BLAHDBG("fqan:  %s\n", fqan);
	return 0;
}

int CEReq_parse(classad_context cad, char* filename)
{
	FILE *req_file=NULL;
	char **reqstr=NULL;
	int cs=0;
	char *vo=NULL;
	req_file= fopen(filename,"w");
	if(req_file==NULL) return 1;
	/**
	int unwind_attributes(classad_context cad, char *attribute_name, char ***results);
	**/
	classad_get_dstring_attribute(cad, "VirtualOrganisation", &vo);
	if(vo)
	{
		cs = fwrite("VirtualOrganisation=", 1, strlen("VirtualOrganisation="), req_file);
		cs = fwrite(vo, 1, strlen(vo), req_file);
		cs = fwrite("\n" ,1, strlen("\n"), req_file);
		free(vo);
	}
	unwind_attributes(cad,"CERequirements",&reqstr);
	while(*reqstr != NULL)
	{
		cs = fwrite(*reqstr ,1, strlen(*reqstr), req_file);
		cs = fwrite("\n" ,1, strlen("\n"), req_file);
		reqstr++;
	}
	fclose(req_file);
	return 0;
}

int check_TransferINOUT(classad_context cad, char **command, char *reqId)
{
        char *resultLine=NULL;
        int result;
        char *tmpIOfilestring = NULL;
        FILE *tmpIOfile = NULL;
        char *tmpstr = NULL;
        char *superbuffer = NULL;
        char *superbufferRemaps = NULL;
        char *superbufferTMP = NULL;
        char *iwd = NULL;
        char  filebuffer[MAX_FILE_LIST_BUFFER_SIZE];
        char  singlefbuffer[MAX_FILE_LIST_BUFFER_SIZE];
        struct timeval ts;
        int i,cs,fc,oc,iwdlen,fbc,slen;
        i=cs=fc=oc=iwdlen=fbc=slen=0;
        char *newptr=NULL;
        char *newptr1=NULL;
        char *newptr2=NULL;
        struct stat tmp_stat;
        /* timetag to have unique Input and Output file lists */
        gettimeofday(&ts, NULL);

        /* write files in InputFileList */
        result = classad_get_dstring_attribute(cad, "TransferInput", &superbuffer);
        if (result == C_CLASSAD_NO_ERROR)
        {
                result = classad_get_dstring_attribute(cad, "Iwd", &iwd);
                if(iwd == NULL)
                {
                        /* PUSH A FAILURE */
                        resultLine = make_message("%s 1 Iwd\\ not\\ found\\ N/A", reqId);
                        enqueue_result(resultLine);
	                free(resultLine);
			return 1;
                }
		iwdlen=strlen(iwd);
                tmpIOfilestring = make_message("%s/%s_%s_%d%d", tmp_dir, "InputFileList",reqId, ts.tv_sec, ts.tv_usec);
                tmpIOfile = fopen(tmpIOfilestring, "w");
                if(tmpIOfile == NULL)
                {
                        /* PUSH A FAILURE */
                        resultLine = make_message("%s 1 Error\\ opening\\ %s\\ N/A", reqId,tmpIOfilestring);
                        free(tmpIOfilestring);
                        enqueue_result(resultLine);
                        free(resultLine);
			return 1;
                }

		if (enable_condor_glexec && fstat(fileno(tmpIOfile), &tmp_stat) >= 0)
		{
			if ((tmp_stat.st_mode & S_IWGRP) == 0)
			{
				if (fchmod(fileno(tmpIOfile),tmp_stat.st_mode|S_IWGRP|S_IRGRP|S_IXGRP)<0)
				{
					fprintf(stderr,"WARNING: cannot make %s group writable: %s\n",
					        tmpIOfilestring, strerror(errno));
				}
			}	
		}

                memset(singlefbuffer,10000,0);
                memset(filebuffer,10000,0);
                slen=strlen(superbuffer);
                for ( i = 0 ; i < slen; )
                {
                        if ((superbuffer[i] == ',')||(i == (slen - 1)))
                        {
                                if (i == (slen - 1))
                                {
                                        superbuffer[++i] ='\n';
                                }
                                /* if the path of the file is not absolute must be added the Iwd directory before it*/
                                if(superbuffer[oc] != '/')
                                {
                                        if(iwd==NULL)
                                        {
                                                /* PUSH A FAILURE */
                                                resultLine = make_message("%s 1 No\\ Iwd\\  avalaible\\ for\\ InputFile\\ N/A", reqId);
                                                free(tmpIOfilestring);
						enqueue_result(resultLine);
                        			free(resultLine);
                                                return 1;
                                        }
                                       memcpy(singlefbuffer,iwd,iwdlen);
                                        singlefbuffer[iwdlen] ='/';
                                        memcpy(&singlefbuffer[iwdlen+1],&superbuffer[oc],i - oc);
                                        singlefbuffer[iwdlen + i - oc + 1] ='\n';
                                }else
                                {
                                        memcpy(&singlefbuffer[iwdlen+1],&superbuffer[oc],i - oc);
                                        singlefbuffer[i - oc + 1] ='\n';
                                }
                                memcpy(&filebuffer[fbc],singlefbuffer,strlen(singlefbuffer));
                                fbc+=strlen(singlefbuffer);
                                fc++;
                                if (i == (slen))
                                {
                                        filebuffer[fbc]=0;
                                        break;
                                }
                                oc=++i;
                        }
                                else i++;
                }
                cs = fwrite(filebuffer,1, strlen(filebuffer), tmpIOfile);
                newptr = make_message("%s -I %s", *command, tmpIOfilestring);

                fclose(tmpIOfile);
                free(tmpIOfilestring);
                free(superbuffer);
        }

        if(newptr==NULL) newptr = *command;
        cs = 0;
        result = classad_get_dstring_attribute(cad, "TransferOutput", &superbuffer);
        if (result == C_CLASSAD_NO_ERROR)
        {

                if(classad_get_dstring_attribute(cad, "TransferOutputRemaps", &superbufferRemaps) == C_CLASSAD_NO_ERROR)
                {
                        superbufferTMP = (char*)malloc(strlen(superbuffer)+1);
                        strcpy(superbufferTMP,superbuffer);
                        superbufferTMP[strlen(superbuffer)]=0;
                }
                tmpIOfilestring = make_message("%s/%s_%s_%d%d", tmp_dir, "OutputFileList",reqId,ts.tv_sec, ts.tv_usec);
                tmpIOfile = fopen(tmpIOfilestring, "w");
                if(tmpIOfile == NULL)
                {
                        /* PUSH A FAILURE */
                        resultLine = make_message("%s 1 Error\\ opening\\  %s\\ N/A", reqId,tmpIOfilestring);
                        free(tmpIOfilestring);
			enqueue_result(resultLine);
                        free(resultLine);
                        return 1;
                }

		if (enable_condor_glexec && fstat(fileno(tmpIOfile), &tmp_stat) >= 0)
		{
			if ((tmp_stat.st_mode & S_IWGRP) == 0)
			{
				if (fchmod(fileno(tmpIOfile),tmp_stat.st_mode|S_IWGRP|S_IRGRP|S_IXGRP)<0)
				{
					fprintf(stderr,"WARNING: cannot make %s group writable: %s\n",
					        tmpIOfilestring, strerror(errno));
				}
			}	
		}

                for(i =0; i < strlen(superbuffer); i++){if (superbuffer[i] == ',')superbuffer[i] ='\n'; }
                cs = fwrite(superbuffer,1 , strlen(superbuffer), tmpIOfile);
                if(strlen(superbuffer) != cs)
                {
                        /* PUSH A FAILURE */
                        resultLine = make_message("%s 1 Error\\ writing\\ in\\ %s\\ N/A", reqId,tmpIOfilestring);
                        free(tmpIOfilestring);
                        free(superbuffer);
                        fclose(tmpIOfile);
                        enqueue_result(resultLine);
                        free(resultLine);
			return 1;
                }
                fwrite("\n",1,1,tmpIOfile);
                newptr1 = make_message("%s -O %s", newptr, tmpIOfilestring);
                fclose(tmpIOfile);
                free(tmpIOfilestring);
                free(superbuffer);
        }
        if(superbufferTMP != NULL)
        {
                superbuffer = outputfileRemaps(superbufferTMP,superbufferRemaps);
                tmpIOfilestring = make_message("%s/%s_%s_%d%d", tmp_dir, "OutputFileListRemaps",reqId,ts.tv_sec, ts.tv_usec);
                tmpIOfile = fopen(tmpIOfilestring, "w");
                if(tmpIOfile == NULL)
                {
                        /* PUSH A FAILURE */
                        resultLine = make_message("%s 1 Error\\ opening\\  %s\\ N/A", reqId,tmpIOfilestring);
                        free(tmpIOfilestring);
			enqueue_result(resultLine);
                        free(resultLine);
                        return 1;
                }

		if (enable_condor_glexec && fstat(fileno(tmpIOfile), &tmp_stat) >= 0)
		{
			if ((tmp_stat.st_mode & S_IWGRP) == 0)
			{
				if (fchmod(fileno(tmpIOfile),tmp_stat.st_mode|S_IWGRP|S_IRGRP|S_IXGRP)<0)
				{
					fprintf(stderr,"WARNING: cannot make %s group writable: %s\n",
					        tmpIOfilestring, strerror(errno));
				}
			}	
		}

                for(i =0; i < strlen(superbuffer); i++){if (superbuffer[i] == ',')superbuffer[i] ='\n'; }
                cs = fwrite(superbuffer,1 , strlen(superbuffer), tmpIOfile);
                if(strlen(superbuffer) != cs)
                {
                        /* PUSH A FAILURE */
                        resultLine = make_message("%s 1 Error\\ writing\\ in\\ %s\\ N/A", reqId,tmpIOfilestring);
                        free(tmpIOfilestring);
                        free(superbuffer);
                        fclose(tmpIOfile);
			enqueue_result(resultLine);
                        free(resultLine);
                        return 1;
                }
                fwrite("\n",1,1,tmpIOfile);
                newptr2 = make_message("%s -R %s", newptr1, tmpIOfilestring);
                fclose(tmpIOfile);
                free(tmpIOfilestring);
                free(superbuffer);
                free(superbufferTMP);
                free(superbufferRemaps);

	}
        if(newptr2)
        {
                 free(newptr1);
                 if(*command != newptr) free(newptr);
                 free(*command);
                 *command = newptr2;
        }
        else
        if(newptr1)
        {
                 if(*command != newptr) free(newptr);
                 free(*command);
                 *command=newptr1;
         }else
         if(*command != newptr)
         {
                free(*command);
                *command=newptr;
         }

        return 0;
}

char*  outputfileRemaps(char *sb,char *sbrmp)
{
        /*
                Files in TransferOutputFile attribute are remapped on TransferOutputFileRemaps
                Example of possible remapping combinations:
                out1,out2,out3,log/out4,log/out5,out6,/tmp/out7
                out2=out2a,out3=/my/data/dir/out3a,log/out5=/my/log/dir/out5a,out6=sub/out6
        */
        char *newbuffer = NULL;
        char *tstr = NULL;
        char *tstr2 = NULL;
        int sblen = strlen(sb);
        int sbrmplen = strlen(sbrmp);
        char *sbtemp = malloc(sblen);
        char *sbrmptemp = malloc(sbrmplen);
        int i = 0, j = 0, endstridx = 0, begstridx = 0, endstridx1 = 0, begstridx1 = 0, strmpd = 0, blen = 0, last = 0;
        for( i = 0; i <= sblen ; i++)
        {
                if((sb[i] == ',')||(i == sblen - 1))
                {
                        /* Files to be remapped are extracted*/
                        if(i < sblen )
                        {
                                endstridx = (i ==(sblen - 1) ? i: i - 1);
                                tstr = malloc(endstridx  - begstridx + 2);
                                strncpy(tstr,&sb[begstridx],endstridx  - begstridx + 1);
                                tstr[endstridx  - begstridx + 1] = '\0';
                                if(i < sblen )
                                begstridx = i + 1;
                        }else
                        {
                                endstridx = i;
                                tstr = malloc(endstridx  - begstridx + 2);
                                strncpy(tstr,&sb[begstridx],endstridx  - begstridx + 1);
                                tstr[endstridx  - begstridx + 1] = '\0';
                                last = 1;
                        }
                        /* Search if the file must be remapped */
                        for(j = 0; j <= sbrmplen;)
                        {
                                endstridx1=0;
                                if(!strncmp(&sbrmp[j],tstr,strlen(tstr)))
                                {
                                        begstridx1 = j + strlen(tstr) + 1;
               /* separator ; */        while((sbrmp[begstridx1 + endstridx1] != ';')&&((begstridx1 + endstridx1) <= sbrmplen ))
                                                 endstridx1++;
                                        if(begstridx1 + endstridx1 <= sbrmplen + 1)
                                        {
                                                tstr2=malloc(endstridx1 + 1);
                                                memset(tstr2,0,endstridx1);
                                                strncpy(tstr2,&sbrmp[begstridx1],endstridx1);
                                                tstr2[endstridx1]='\0';
                                                strmpd = 1;
                                        }
                                        begstridx1 = 0;
                                        break;
                                }else j++;
                        }
                        if(newbuffer)
                                blen = strlen(newbuffer);
                        if(strmpd == 0)
                        {
                                newbuffer =(char*) realloc((void*)newbuffer, blen + strlen(tstr) + 2);
                                strcpy(&newbuffer[blen],tstr);
                                newbuffer[blen + strlen(tstr)] = (last? 0: ',');
                                newbuffer[blen + strlen(tstr) + 1] = '\0';
                                memset((void*)tstr,0,strlen(tstr));
                        }
                        else
                        {
                                newbuffer = (char*) realloc((void*)newbuffer, blen + strlen(tstr2) + 2);
                                strcpy(&newbuffer[blen],tstr2);
                                newbuffer[blen + strlen(tstr2)] = (last? 0: ',');
                                newbuffer[blen + strlen(tstr2) + 1] = '\0';
                                memset((void*)tstr2,0,strlen(tstr2));
                                memset((void*)tstr,0,strlen(tstr));
                        }
                        strmpd = 0;
                        if (tstr2){free(tstr2); tstr2=NULL;}
                        if (tstr) {free(tstr); tstr=NULL;}
                }
        }

        return newbuffer;
}

#define SINGLE_QUOTE_CHAR '\''
#define DOUBLE_QUOTE_CHAR '\"'
#define CONVARG_OPENING        "\"\\\""
#define CONVARG_OPENING_LEN    3
#define CONVARG_CLOSING        "\\\"\"\000"
#define CONVARG_CLOSING_LEN    4
#define CONVARG_QUOTSEP        "\\\"%c\\\""
#define CONVARG_QUOTSEP_LEN    5
#define CONVARG_DBLQUOTESC     "\\\\\\\""
#define CONVARG_DBLQUOTESC_LEN 4

char*
ConvertArgs(char* original, char separator)
{
	/* example arguments
	args(1)="a '''b''' 'c d' e' 'f ''''" ---> "a;\'b\';c\ d;e\ f;\'"
	args(1)="'''b''' 'c d'" --> "\'b\';c\ d"
	*/

	int inside_quotes = 0;
	char *result, *short_result;
	int i, j;
	int orig_len;

	char quoted_sep[CONVARG_QUOTSEP_LEN];
	sprintf(quoted_sep, CONVARG_QUOTSEP, separator);

	orig_len = strlen(original);
	if (orig_len == 0) return NULL;

        /* Worst case is <a> --> <"\"a\""> */
	result = (char *)malloc(orig_len * 10);
	if (result == NULL) return NULL;

	memcpy(result, CONVARG_OPENING, CONVARG_OPENING_LEN);
	j = CONVARG_OPENING_LEN;

	for(i=0; i < orig_len; i++)
	{
		if(original[i] == SINGLE_QUOTE_CHAR && !inside_quotes)
		{	/* the quote is an opening quote */
			inside_quotes = 1;
		}
		else if (original[i] == SINGLE_QUOTE_CHAR)
		{	/* a quote inside quotes... */
			if ((i+1) < orig_len && original[i+1] == SINGLE_QUOTE_CHAR) 
			{	/* the quote is a literal, copy and skip */
				result[j++] = original[i++];
			}
			else
			{	/* the quote is a closing quote */
				inside_quotes = 0;
			}
		}
		else if (original[i] == ' ')
		{	/* a blank... */
			if (inside_quotes)
			{	/* the blank is a literal, copy */
				result[j++] = original[i];
			}
			else
			{	/* the blank is a separator */
				memcpy(result + j, quoted_sep, CONVARG_QUOTSEP_LEN);
				j += CONVARG_QUOTSEP_LEN;
			}
		}
		else if (original[i] == DOUBLE_QUOTE_CHAR)
		{	/* double quotes need to be triple-escaped to make it to the submit file */
			memcpy(result + j, CONVARG_DBLQUOTESC, CONVARG_DBLQUOTESC_LEN);
			j += CONVARG_DBLQUOTESC_LEN;
		}
		else
		{	/* plain copy from the original */
			result[j++] = original[i];
		}
	}
	memcpy(result + j, CONVARG_CLOSING, CONVARG_CLOSING_LEN);

	return(result);
}

