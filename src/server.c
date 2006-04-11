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
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "blahpd.h"
#include "classad_c_helper.h"
#include "commands.h"
#include "job_status.h"
#include "resbuffer.h"
#include "mtsafe_popen.h"

#define COMMAND_PREFIX "-c"
#define PERSISTENT_BUFFER BUFFER_DONT_SAVE
#define JOBID_PREFIX            "BLAHP_JOBID_PREFIX"
#define JOBID_PREFIX_LEN        18
#define HOLD_JOB                1
#define RESUME_JOB              0
#define MAX_LRMS_NUMBER 	10
#define MAX_LRMS_NAME_SIZE	4
#define MAX_CERT_SIZE		100000
#define MAX_TEMP_ARRAY_SIZE              1000
#define MAX_FILE_LIST_BUFFER_SIZE        10000
#define MAX_CONF_FILE_SIZE               100000
 
t_resline *first_job = NULL;
t_resline *last_job = NULL;
int num_jobs = 0;

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

const char *opt_format[4] = {
	" %s %s",               /* NO_QUOTE */
	" %s \"%s\"",           /* SINGLE_QUOTE */
	" %s \"\\\"%s\\\"\"",   /* DOUBLE_QUOTE */
	" %s %d"                /* INT_NOQUOTE */
};

char* statusstring[]={
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
int limit_proxy(char* proxyname);
int getProxyInfo(char* proxname, char* fqan, char* userDN);
int  logAccInfo(char* jobId, char* server_lrms, classad_context cad, char* fqan, char* userDN);
int CEReq_parse(classad_context cad, char* filename);

/* Global variables */
static int server_socket;
static int async_mode = 0;
static int async_notice = 0;
static int exit_program = 0;
static pthread_mutex_t send_lock  = PTHREAD_MUTEX_INITIALIZER;
/* char *server_lrms; */
char *blah_script_location;
char *blah_version;
char lrmslist[MAX_LRMS_NUMBER][MAX_LRMS_NAME_SIZE];
int  lrms_counter = 0;
int  glexec_mode = 0;
char *bssp = NULL;
char *gloc = NULL;
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
	fd_set readfs;
	int exitcode = 0;
	int reply_len;
	pthread_t task_tid;
	int i, argc;
	char **argv;
	command_t *command;
	FILE *conffile = NULL;
	char *conffilestr = NULL;
	char buffer[128];
	int bc=0;
	char *needed_libs=NULL;
	char *old_ld_lib=NULL;
	char *new_ld_lib=NULL;
	init_resbuffer();
	if (cli_socket == 0) server_socket = 1;
	else                 server_socket = cli_socket;

	/* Get values from environment */
	if ((result = getenv("GLITE_LOCATION")) == NULL)
	{
		result = DEFAULT_GLITE_LOCATION;
	}

	needed_libs = make_message("%s/lib:%s/externals/lib:%s/lib:/opt/lcg/lib", result, result, getenv("GLOBUS_LOCATION") ? getenv("GLOBUS_LOCATION") : "/opt/globus");
        old_ld_lib=getenv("LD_LIBRARY_PATH");
        if(old_ld_lib)
        {
        	new_ld_lib =(char*)malloc(strlen(old_ld_lib) + 2 + strlen(needed_libs));
          	sprintf(new_ld_lib,"%s;%s",old_ld_lib,needed_libs);
          	setenv("LD_LIBRARY_PATH",new_ld_lib,1);
        }else
         	 setenv("LD_LIBRARY_PATH",needed_libs,1);
	
	blah_script_location = make_message(BINDIR_LOCATION, result);
	blah_version = make_message(RCSID_VERSION, VERSION, "poly");
        if ((gloc=getenv("GLEXEC_COMMAND")) == NULL)
        {
        	gloc = DEFAULT_GLEXEC_COMMAND;
        }
	conffilestr = make_message("%s/etc/blah.config",result);
	if((conffile = fopen(conffilestr,"r")) != NULL)
	{
		while(fgets(buffer, 128, conffile))
		{
			if (!strncmp (buffer,"supported_lrms=", strlen("supported_lrms=")))
			{
				bc+=strlen("supported_lrms=");
                                while(strlen(&buffer[bc]) > 0)
                                {
                                        strncpy(lrmslist[lrms_counter],&buffer[bc],3);
                                        lrms_counter++;
					if(strlen(&buffer[bc]) > 3) bc+=4;
					else 
					   break;
                                }
			}
			bc=0;
			memset(buffer,0,128);
		}
		
		fclose(conffile);
		free(conffilestr);
	}
	
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

			if (command)
			{
				if (argc != (command->required_params + 1))
				{
					reply = make_message("E expected\\ %d\\ parameters,\\ %d\\ found\\r\n", command->required_params, argc -1);
				}
				else
				{
					if (command->threaded)
					{
						if(pthread_create(&task_tid, NULL, command->cmd_handler, (void *)argv))
						{
							reply = make_message("F Cannot\\ start\\ thread\r\n");
						}
						else
						{
							reply = make_message("S\r\n");
							pthread_detach(task_tid);
						}
						/* free argv in threaded function */
					}
					else
					{
						if ((result = (char *)command->cmd_handler(argv)) == NULL)
						{
							reply = make_message("F\r\n");
						}
						else
						{
							reply = make_message("S %s\r\n", result);
							free(result);
						}
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
				write(server_socket, "E Cannot\\ allocate\\ return\\ line\r\n", 34);
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
	result = strdup("Server\\ exiting");
	return(result);
}

void *
cmd_commands(void *args)
{
	char *result;

	result = known_commands();
	return(result);
}

void *
cmd_version(void *args)
{
	char *result;

	result = strdup(blah_version);
	return(result);	
}

void *
cmd_async_on(void *args)
{
	char *result;

	async_mode = async_notice = 1;
	result = strdup("Async\\ mode\\ on");
	return(result);
}
			
void *
cmd_async_off(void *args)
{
	char *result;

	async_mode = async_notice = 0;
	result = strdup("Async\\ mode\\ off");
	return(result);
}

void *
cmd_results(void *args)
{
	char *result;
	char *res_lines;
	char *tmp_realloc;

	if (result = (char *) malloc (13)) /* hope 10 digits suffice*/
	{
		snprintf(result, 10, "%d", num_results());
		if(num_results())
		{
			strcat(result, "\r\n");
			res_lines = get_lines(BUFFER_FLUSH);
			if ((tmp_realloc = (char *)realloc(result, strlen(result) + strlen(res_lines) + 2)) == NULL)
			{
				free(result);
				free(res_lines);
				return(NULL);
			}
			result = tmp_realloc;
			strcat(result, res_lines);
			free(res_lines);
		}

		/* From now on, send 'R' when a new resline is enqueued */
		async_notice = async_mode;
	}
	
	/* If malloc has failed, return NULL to notify error */
	return(result);
}

void *
cmd_set_glexec_dn(void *args)
{

        char *result  = NULL;
        struct stat buf;
        char **argv = (char **)args;
	char *proxt4= argv[1];
        char *ssl_client_cert = argv[2];
	FILE *dummy;
        char certbuffer[MAX_CERT_SIZE];
	int res = 0;
	char *cmdstr = NULL;
	char *proxynameNew = NULL;
	
	if((!stat(proxt4, &buf))&&(!stat(ssl_client_cert, &buf)))
        {
                res = setenv("GLEXEC_MODE","lcmaps_get_account",1);
                if (res)
                {
                        result=strdup("Unable\\ to\\ set\\ GLEXEC\\ MODE");
                        return(result);
                }
		res = setenv("BLAHP_SAVE_SOURCE_PROXY",proxt4,1);
                if (res)
                {
			result=strdup("Unable\\ to\\ set\\ GLEXEC\\ MODE");
                        return(result);
                }
		dummy = fopen(ssl_client_cert, "r");
		if (dummy == NULL)
		{ 
			unsetenv("BLAHP_SAVE_SOURCE_PROXY");
			result=strdup("Unable\\ to\\ read\\ SSL\\ CLIENT\\ CERT");
			return(result);
		}

		/* reads from cert file and stores in certbuffer */
		fread(certbuffer, MAX_CERT_SIZE, 1, dummy);
		fclose(dummy);
		if(strlen(certbuffer) > 0)
		{
			/* save blah_save_source_proxy */
			res = setenv("SSL_CLIENT_CERT",certbuffer,1);
			/* proxt4 must be limited for subsequent submission */		
               		proxynameNew=make_message("%s.lmt",proxt4);
               		cmdstr=make_message("cp %s %s",proxt4, proxynameNew);
               		result=system(cmdstr);
                        if(result)
                        {
                        	result = make_message("Error\\ reading\\ proxy\\ %s", proxt4);
                        	free(proxynameNew);
                        	free(cmdstr);
                        	unsetenv("SSL_CLIENT_CERT");
				return(result);
                        }               		


			limit_proxy(proxynameNew);
               		if(bssp) free(bssp);
			bssp = strdup(proxynameNew);
			free(cmdstr);
			free(proxynameNew);
		}
		else
                {
                        result=strdup("Unable\\ to\\ read\\ SSL\\ CLIENT\\ CERT");
                	glexec_mode = 0;                        
			return(result);
                }
		glexec_mode = 1;
                result = strdup("Glexec\\ mode\\ on");
	}else
                result = strdup("Proxy\\ file\\ not\\ found");

	return(result);
}

void *
cmd_unset_glexec_dn(void *args)
{
        char *result;

        unsetenv("GLEXEC_MODE");
        unsetenv("BLAHP_SAVE_SOURCE_PROXY");
        unsetenv("SSL_CLIENT_CERT");
	glexec_mode = 0;
        if(bssp!=NULL)
	{
		free(bssp);
		bssp = NULL;
	}
	result=make_message("Glexec\\ mode\\ off");
        return (result);
}

/* Threaded commands
 * N.B.: functions must free argv before return
 * */

void *
cmd_submit_job(void *args)
{
	const char *submission_cmd;
	FILE *cmd_out;
	int retcod;
	char *command;
	char jobId[JOBID_MAX_LEN];
	char *resultLine;
	char **argv = (char **)args;
	char **arg_ptr;
	classad_context cad;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	char *server_lrms = NULL;
	int result;
	char error_message[ERROR_MAX_LEN];
	char *error_string;
	int res = 1;
	char* resfg = NULL;
	char *proxyname   = NULL;
        char *proxynameNew   = NULL;
	char *cmdstr = NULL;
	char *command_ext = NULL;
        char fqan[MAX_TEMP_ARRAY_SIZE], userDN[MAX_TEMP_ARRAY_SIZE];
	int count=0;
	int enable_log=0;
        struct timeval ts;
	char *ce_req=NULL;
        char *req_file=NULL;
	cad = classad_parse(jobDescr);
	if (cad == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Error\\ parsing\\ classad N/A", reqId);
		goto cleanup_argv;
	}


	/* Get the lrms type from classad attribute "gridtype" */
	result = classad_get_dstring_attribute(cad, "gridtype", &server_lrms);
	if (result != C_CLASSAD_NO_ERROR)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Missing\\ gridtype\\ in\\ submission\\ classAd N/A", reqId);
		goto cleanup_cad;
	}

	if(glexec_mode)
	{
		setenv("GLEXEC_SOURCE_PROXY",bssp,1);
		result = classad_get_dstring_attribute(cad, "x509UserProxy", &proxyname);
                if (result == C_CLASSAD_NO_ERROR)
		{
			setenv("GLEXEC_TARGET_PROXY",proxyname,1);
		}else{
                       /* PUSH A FAILURE */
                       resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
                       free(server_lrms);
                       unsetenv("GLEXEC_SOURCE_PROXY");
                       unsetenv("GLEXEC_TARGET_PROXY");
                       enqueue_result(resultLine);
                       return;        
	}
		command = make_message("%s %s/%s_submit.sh -x %s", gloc, blah_script_location, server_lrms, proxyname);
	}else
		command = make_message("%s/%s_submit.sh", blah_script_location, server_lrms);


	if (command == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Out\\ of\\ Memory N/A", reqId);
		goto cleanup_lrms;
	}

        /* Get the proxy name from classad attribute "X509UserProxy" */
        if (((result = classad_get_dstring_attribute(cad, "x509UserProxy", &proxyname)) == C_CLASSAD_NO_ERROR)
	   &&(!glexec_mode ))
        { 
	       proxynameNew=make_message("%s.lmt",proxyname);
               cmdstr=make_message("cp %s %s",proxyname, proxynameNew);
               system(cmdstr);
               limit_proxy(proxynameNew);
	       command_ext = make_message("%s -x %s", command, proxynameNew);
               //free(proxyname);
               free(cmdstr);
               if (command_ext == NULL)
               {
                       /* PUSH A FAILURE */
                       resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
                       free(proxynameNew);
                       goto cleanup_command;
               }

               /* Swap new command in */
               free(command);
               command = command_ext;
               free(proxynameNew);
	}else if(result != C_CLASSAD_NO_ERROR) /* job must be refused */
        {
        	/* PUSH A FAILURE */
        	resultLine = make_message("%s 1 Proxy\\ not\\ found N/A", reqId);
        	goto cleanup_command;   
	}

	/* Cmd attribute is mandatory: stop on any error */
	if (set_cmd_string_option(&command, cad, "Cmd", COMMAND_PREFIX, NO_QUOTE) != C_CLASSAD_NO_ERROR)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 7 Cannot\\ parse\\ Cmd\\ attribute\\ in\\ classad N/A", reqId);
		goto cleanup_command;
	}

        if((result = classad_get_dstring_attribute(cad, "CERequirements", &ce_req)) == C_CLASSAD_NO_ERROR)
        {
                gettimeofday(&ts, NULL);
                req_file = make_message("ce-req-file-%d%d", ts.tv_sec, ts.tv_usec);
                if(!CEReq_parse(cad,req_file))
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
                        free(req_file);
                }
        }

	/* All other attributes are optional: fail only on memory error 
	   IMPORTANT: Args must alway be the last!
	*/
	if ((set_cmd_string_option(&command, cad, "In",         "-i", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Out",        "-o", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Err",        "-e", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Iwd",        "-w", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Env",        "-v", SINGLE_QUOTE)  == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Queue",      "-q", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_int_option   (&command, cad, "NodeNumber", "-n", INT_NOQUOTE)   == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_bool_option  (&command, cad, "StageCmd",   "-s", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "ClientJobId","-j", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Args",      	"--", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY))
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
		goto cleanup_command;
	}
        /* DGAS accounting */
        if(proxyname)
        {
                if(getProxyInfo(proxyname, fqan, userDN))
                {
                        /* PUSH A FAILURE */
                        resultLine = make_message("%s 1 Credentials\\ not\\ valid N/A", reqId);
                        goto cleanup_command;
                }
                if(userDN) enable_log=1;
                free(proxyname);
        }

	/* Execute the submission command */
	/* fprintf(stderr, "DEBUG: submission cmd = '%s'\n", command); */
	if((cmd_out = mtsafe_popen(command, "r")) == NULL)
	{
		/* PUSH A FAILURE */
		/* errno is not set if popen fails because of low memory */
		if (retcod = errno)
			strerror_r(errno, error_message, sizeof(error_message));
		else
			strncpy(error_message, "Cannot open pipe for the command: out of memory", sizeof(error_message));

		error_string = escape_spaces(error_message);
		resultLine = make_message("%s %d %s", reqId, retcod, error_string);
		free(error_string);
		goto cleanup_command;
	}
	
	while(1)
	{
	 	resfg = fgets(jobId, sizeof(jobId), cmd_out);
		if (resfg == NULL) break;
		res = strncmp(jobId,JOBID_PREFIX,JOBID_PREFIX_LEN);
		if (res == 0) break;
	}

	retcod = mtsafe_pclose(cmd_out);
	if ((retcod != 0)||(res != 0))
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 2 Submit\\ command\\ didn't\\ return\\ jobId\\ (exit\\ code\\ =\\ %d) N/A", reqId, WEXITSTATUS(retcod));
		goto cleanup_command;
	}
	
	if (jobId[strlen(jobId) - 1] == '\n') jobId[strlen(jobId) - 1] = '\000';
 	
	/* PUSH A SUCCESS */
	resultLine = make_message("%s 0 No\\ error %s", reqId, jobId + JOBID_PREFIX_LEN);
	
	/* DGAS accounting */
        if(enable_log)
                logAccInfo(jobId, server_lrms, cad, fqan, userDN);        

	/* Free up all arguments and exit (exit point in case of error is the label
           pointing to last successfully allocated variable) */
cleanup_command:
	free(command);
cleanup_lrms:
	free(server_lrms);
cleanup_cad:
	classad_free(cad);
cleanup_argv:
	free_args(argv);
	enqueue_result(resultLine);
	free(resultLine);
	/* To be fixed in a safe way
	if(glexec_mode)
	{
		unsetenv("GLEXEC_SOURCE_PROXY");
        	unsetenv("GLEXEC_TARGET_PROXY");
	}
	*/
	return;
}

void *
cmd_cancel_job(void* args)
{
	int retcod;
	FILE *dummy;
	char *command;
	char *resultLine = NULL;
	char **argv = (char **)args;
	char **arg_ptr;
	char *server_lrms;
	char *reqId = argv[1];
	char *jobId;
	char error_message[1024];
	char *error_string;
	char answer[1024];

	/* The job Id needs at least 4 chars for the "<lrms>/" prefix */
	if (strlen(argv[2]) < 4)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 2 Malformed\\ jobId %s", reqId, jobId);
		goto cleanup_argv;
	}

	/* Split <lrms> and actual job Id */
	if((server_lrms = strdup(argv[2])) == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Cannot\\ allocate\\ memory\\ for\\ the\\ lrms\\ string", reqId);
		goto cleanup_argv;
	}
	server_lrms[3] = '\0';
	jobId = server_lrms + 4;

	/* Prepare the cancellation command */
	if(glexec_mode )
	{
        	command = make_message("%s %s/%s_cancel.sh %s", gloc, blah_script_location, server_lrms, jobId);
	}else
	{
		command = make_message("%s/%s_cancel.sh %s", blah_script_location, server_lrms, jobId);
	}
	if (command == NULL)
	{
		/* PUSH A FAILURE */
		resultLine = make_message("%s 1 Cannot\\ allocate\\ memory\\ for\\ the\\ command\\ string", reqId);
		goto cleanup_lrms;
	}

	/* Execute the command */
	/* fprintf(stderr, "DEBUG: executing %s\n", command); */
	if((dummy = mtsafe_popen(command, "r")) == NULL)
	{
		/* PUSH A FAILURE */
		/* errno is not set if popen fails because of low memory */
		if (retcod = errno)
			strerror_r(errno, error_message, sizeof(error_message));
		else
			strncpy(error_message, "Cannot open pipe for the command: out of memory", sizeof(error_message));

		error_string = escape_spaces(error_message);
		resultLine = make_message("%s %d %s", reqId, retcod, error_string);
		free(error_string);
		goto cleanup_command;
	}	

       /* Multiple job cancellation */
       while(fgets(answer, sizeof(answer), dummy))
       {
               answer[strlen(answer) - 1] = 0;
               resultLine = make_message("%s%s", reqId, answer);
               enqueue_result(resultLine);
               free(resultLine);
               resultLine=NULL;
       }

	retcod = mtsafe_pclose(dummy);

	/* Free up all arguments and exit (exit point in case of error is the label
           pointing to last successfully allocated variable) */
cleanup_command:
	free(command);
cleanup_lrms:
	free(server_lrms);
cleanup_argv:
	free_args(argv);
        if(resultLine)
        {
              enqueue_result(resultLine);
              free (resultLine);
        }
	return;
}

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

        retcode = get_status(jobDescr, status_ad, errstr, 1, &job_number);
        if (!retcode)
        {
                for(i = 0;i < job_number; i++)
                {
                        esc_errstr = escape_spaces(errstr[i]);
                        if(status_ad[i] != NULL )
                        {
                                        classad_get_int_attribute(status_ad[i], "JobStatus", &jobStatus);
                                        str_cad = classad_unparse(status_ad[i]);
                                        esc_str_cad = escape_spaces(str_cad);
                                        if(job_number > 1)
                                                resultLine = make_message("%s.%d %d  %d %s", reqId, i,retcode, jobStatus, esc_str_cad);
                                        else
                                                resultLine = make_message("%s %d  %d %s", reqId, retcode, jobStatus, esc_str_cad);
                                        classad_free(status_ad[i]);
                                        free(str_cad);
                                        free(esc_str_cad);
                        }else
                        {
                                        if(job_number > 1)
                                                resultLine = make_message("%s.%d 1 %s", reqId, i, esc_errstr);
                                        else
                                                resultLine = make_message("%s 1 %s" ,reqId, esc_errstr);
                        }
                        free(esc_errstr);
                        enqueue_result(resultLine);
                        free(resultLine);
                }
        }
        else
        {
                resultLine = make_message("%s %d %s N/A", reqId, retcode, errstr[i]);
                enqueue_result(resultLine);
                free(resultLine);
        }
        /* Free up all arguments */
        free_args(argv);
        return;
}


void *
cmd_renew_proxy(void *args)
{
	classad_context status_ad[MAX_JOB_NUMBER];
	char *resultLine;
	char **argv = (char **)args;
	char **arg_ptr;
	char errstr[MAX_JOB_NUMBER][ERROR_MAX_LEN];
	char *esc_errstr;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	char *proxyFileName = argv[3];
	char *workernode;
	char *command=NULL;
	char *proxy_link=NULL;
	//char old_proxy[FILENAME_MAX];
	char old_proxy[MAX_TEMP_ARRAY_SIZE];
	char real_home[MAX_TEMP_ARRAY_SIZE];
	char *temp_str=NULL;
	char *res_str=NULL;
	
	int jobStatus, retcod, result;
	FILE *dummy;
	char error_message[ERROR_MAX_LEN];
	char *error_string;
	char proxyFileNameNew[MAX_TEMP_ARRAY_SIZE];
	int  job_number;

	retcod = get_status(jobDescr, status_ad, errstr, 1, &job_number);
	if(!glexec_mode)
	{
		sprintf(proxyFileNameNew, "%s.lmt",proxyFileName);
	}else
		sprintf(proxyFileNameNew, "%s",proxyFileName);
	if (!strcmp(errstr[0],"No Error"))
	{
			esc_errstr = escape_spaces(errstr[0]);
			classad_get_int_attribute(status_ad[0], "JobStatus", &jobStatus);
			jobDescr = strrchr(jobDescr, '/') + 1;
			switch(jobStatus)
			{
				case 1: /* job queued */
					/* copy the proxy locally */
					if(!glexec_mode)
					{
						proxy_link = make_message("%s/.blah_jobproxy_dir/%s.proxy", getenv("HOME"), jobDescr);
						result = readlink(proxy_link, old_proxy, sizeof(old_proxy) - 1);
					}
					else
					{
						/* /bin/echo */
                				setenv("GLEXEC_SOURCE_PROXY",bssp,1);
						setenv("GLEXEC_TARGET_PROXY",proxyFileNameNew,1);
						temp_str=make_message("%s /bin/pwd", gloc);
                				if ((dummy=mtsafe_popen(temp_str, "r")) == NULL)
						{
                                                	resultLine = make_message("%s 1 Error\\ reading\\ proxy\\ %s", reqId, proxyFileName);
                                                       	free(temp_str);
                                                       	break;
						}
                				res_str=fgets(real_home, MAX_TEMP_ARRAY_SIZE, dummy);
                				mtsafe_pclose(dummy);
						dummy=NULL;
                				if(temp_str) free(temp_str);
                				real_home[strlen(real_home)-1] = 0;
						if(res_str)
						{
							res_str=NULL;
							proxy_link = make_message("%s/.blah_jobproxy_dir/%s.proxy", real_home, jobDescr);
                                                	/* /usr/bin/readlink  */
                                               		temp_str=make_message("%s /usr/bin/readlink %s", gloc, proxy_link);
                                                	if ((dummy=mtsafe_popen(temp_str, "r")) == NULL)
                                                	{
                                                        	resultLine = make_message("%s 1 Error\\ reading\\ proxy\\ link %s", reqId, proxy_link);
                                                        	free(temp_str);
                                                        	break;
                                                	}
                                                	res_str=fgets(old_proxy,MAX_TEMP_ARRAY_SIZE, dummy);
							mtsafe_pclose(dummy);
                                                	dummy=NULL;
                                                	free(temp_str);
							if(old_proxy[0]==0) result=-1;
							else
							{
                                                		old_proxy[strlen(old_proxy)-1] = 0;
								result=0;
							}
						}
					}
					
					if (result == -1)
						resultLine = make_message("%s 1 Error\\ locating\\ original\\ proxy", reqId);
					else
					{
                                        	if(!glexec_mode)
                                                {
								/* proxy must be copied and (if we are not in glexec_mode) limited */
        							command = make_message("cp -f %s %s &>2 /dev/null;",proxyFileName, old_proxy);
								if((dummy = mtsafe_popen(command, "r")) == NULL)
								{
                                                                	resultLine = make_message("%s 1 Error\\ reading\\ proxy\\ %s", reqId, proxyFileName);
                                                                	free(command);
									break;
								}
								limit_proxy(old_proxy);
						}else
						{
						                /* proxy must be copied and (if we are not in glexec_mode) limited */
                                                                command = make_message("%s cp -f %s %s &>2 /dev/null", gloc, proxyFileName, old_proxy);
                                                                if((dummy = mtsafe_popen(command, "r")) == NULL)
                                                                {
                                                                        resultLine = make_message("%s 1 Error\\ reading\\ proxy\\ %s", reqId, proxyFileName);
                                                                        free(command);
                                                                        command=NULL;
									break;
                                                                }
                                                }
						if(command) free(command);
						resultLine = make_message("%s 0 Proxy\\ renewed", reqId);
					}
					if(proxy_link) free(proxy_link);
					break;
				case 2: /* job running */
					/* send the proxy to remote host */
					if (((result = classad_get_dstring_attribute(status_ad[0], "WorkerNode", &workernode)) == C_CLASSAD_NO_ERROR)&&(strcmp(workernode,"")))
					{
                                                if(!glexec_mode)
                                                {
							/* proxy must be limited */
        						command = make_message("cp %s %s &>2 /dev/null",proxyFileName, proxyFileNameNew);
							if((dummy = mtsafe_popen(command, "r")) == NULL)
                                                	{
                                                		resultLine = make_message("%s 1 Error\\ reading\\ proxy\\ %s", reqId, proxyFileName);
                                                		free(command);
								command=NULL;
                                                		break;
                                                	}else
                                                        mtsafe_pclose(dummy);
							limit_proxy(proxyFileNameNew);
						}else
						{
							setenv("GLEXEC_SOURCE_PROXY",bssp,1);
							setenv("GLEXEC_TARGET_PROXY",proxyFileNameNew,1);
						}
						
						
						if(command) free(command);
						command = make_message("export LD_LIBRARY_PATH=%s/lib; %s %s/BPRclient %s %s %s &>2 /dev/null;sleep 5",
				                        getenv("GLOBUS_LOCATION") ? getenv("GLOBUS_LOCATION") : "/opt/globus", glexec_mode ? gloc : " ",
				                        blah_script_location, proxyFileNameNew, jobDescr, workernode);
						free(workernode);
						/* Execute the command */
						/* fprintf(stderr, "DEBUG: executing %s\n", command); */
						if((dummy = mtsafe_popen(command, "r")) == NULL)
						{
							/* PUSH A FAILURE */
							/* errno is not set if popen fails because of low memory */
							if (retcod = errno)
								strerror_r(errno, error_message, sizeof(error_message));
							else
								strncpy(error_message, "Cannot open pipe for the command: out of memory", sizeof(error_message));

							error_string = escape_spaces(error_message);
							resultLine = make_message("%s %d %s", reqId, retcod, error_string);
						}	
						else
						{
							retcod = mtsafe_pclose(dummy);
							resultLine = make_message("%s %d %s", reqId, retcod, retcod ? "Error" : "No\\ error");
						}
						if(command) { free(command); command=NULL;}
					}else
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
					resultLine = make_message("%s 1 Wrong\\ state\\ (%d)", reqId, jobStatus);
			}
		}else
			resultLine = make_message("%s %d %s", reqId, 1, escape_spaces(errstr[0]));
		
		if (resultLine)
		{
			enqueue_result(resultLine);
			free(resultLine);
		}
		else
		{
			enqueue_result("Missing result line due to memory error");
			free(esc_errstr);
		}
	
	/* Free up all arguments */
	if (status_ad[0]) classad_free(status_ad[0]);
	free_args(argv);
	return;
}

void
hold_res_exec(char* jobdescr, char* reqId,char* action,int status )
{
        int retcod;
        FILE *dummy;
        char *command;
        char *resultLine = NULL;
        char *server_lrms;
        char *jobId;
        char error_message[1024];
        char *error_string;

        /* The job Id needs at least 4 chars for the "<lrms>/" prefix */
        if (strlen(jobdescr) < 4)
        {
                /* PUSH A FAILURE */
                resultLine = make_message("%s 2 Malformed\\ jobId %s", reqId, jobId);
                goto cleanup_argv;
        }

        /* Split <lrms> and actual job Id */
        if((server_lrms = strdup(jobdescr)) == NULL)
        {
                /* PUSH A FAILURE */
                resultLine = make_message("%s 1 Cannot\\ allocate\\ memory\\ for\\ the\\ lrms\\ string", reqId);
                goto cleanup_argv;
        }
        server_lrms[3] = '\0';
        jobId = server_lrms + 4;

        if(glexec_mode )
        {
                if(!strcmp(action,"hold"))
                {
                        command = make_message("%s %s/%s_%s.sh %s %d", gloc, blah_script_location, server_lrms, action, jobId, status);
                }else
                {
                        command = make_message("%s %s/%s_%s.sh %s", gloc, blah_script_location, server_lrms, action, jobId);
                }
        }else
        {
                if(!strcmp(action,"hold"))
                {
                        command = make_message("%s/%s_%s.sh %s %d", blah_script_location, server_lrms, action, jobId, status);
                }else
                {
                        command = make_message("%s/%s_%s.sh %s", blah_script_location, server_lrms, action, jobId);
                }
        }

        if (command == NULL)
        {
                /* PUSH A FAILURE */
                resultLine = make_message("%s 1 Cannot\\ allocate\\ memory\\ for\\ the\\ command\\ string", reqId);
                goto cleanup_lrms;
        }

        /* Execute the command */
        /* fprintf(stderr, "DEBUG: executing %s\n", command); */
        if((dummy = mtsafe_popen(command, "r")) == NULL)
        {
                /* PUSH A FAILURE */
                /* errno is not set if popen fails because of low memory */
                if (retcod = errno)
                        strerror_r(errno, error_message, sizeof(error_message));
                else
                        strncpy(error_message, "Cannot open pipe for the command: out of memory", sizeof(error_message));

                error_string = escape_spaces(error_message);
                resultLine = make_message("%s %d %s", reqId, retcod, error_string);
                free(error_string);
                goto cleanup_command;
        }
        retcod = mtsafe_pclose(dummy);
        if(retcod)
        {
                resultLine = make_message("%s %d Job\\ %s:\\ %s\\ not\\ supported\\ by\\ %s", reqId, retcod, statusstring[status - 1] ,action ,server_lrms );
        }else
        resultLine = make_message("%s %d No\\ error", reqId, retcod);
        /* Free up all arguments and exit (exit point in case of error is the label
           pointing to last successfully allocated variable) */
cleanup_command:
        free(command);
cleanup_lrms:
        free(server_lrms);
cleanup_argv:
        enqueue_result(resultLine);
        free (resultLine);

        return;
}

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

        /* job status check */
        retcode = get_status(dummyargv, status_ad, errstr, 0, &job_number);
        /* if multiple jobs are present their id must be extracted from argv[2] */
        i=0;
        tmpjobdescr = strtok(dummyargv," ");
        memcpy(jobdescr[0],tmpjobdescr,strlen(tmpjobdescr));
        if(job_number>1)
        {
                for (i =1; i < job_number;i++)
                {
                        tmpjobdescr= strtok(NULL," ");
                        memcpy(jobdescr[i],tmpjobdescr,strlen(tmpjobdescr));
                }
        }

        if (!retcode)
        {
                for (i=0;i<job_number;i++)
                {
                        if(job_number>1)
                        {
                                reqId= make_message("%s.%d",argv[1],i);
                        }else
				reqId= strdup(argv[1]);
                        if(classad_get_int_attribute(status_ad[i], "JobStatus", &jobStatus)==C_CLASSAD_NO_ERROR)
                        {
                                switch(jobStatus)
                                {
                                        case 1:/* IDLE */
                                                if(action == HOLD_JOB)
                                                {
                                                        hold_res_exec(jobdescr[i],reqId,"hold",1);
                                                }
                                                else
                                                if (resultLine = make_message("%s 1 Job\\ Idle\\ jobId %s", reqId,jobdescr[i]))
                                                {
                                                        enqueue_result(resultLine);
                                                        free(resultLine);
                                                }
                                        break;
                                        case 2:/* RUNNING */
                                                if(action == HOLD_JOB)
                                                {
                                                        hold_res_exec(jobdescr[i],reqId,"hold",2);
                                                }else
                                                if (resultLine = make_message("%s 1 \\ Job\\ Running\\ jobId %s", reqId, jobdescr[i]))
                                                {
                                                        enqueue_result(resultLine);
                                                        free(resultLine);
                                                }
                                        break;
                                        case 3:/* REMOVED */
                                                if (resultLine = make_message("%s 1 Job\\ Removed\\ jobId %s", reqId, jobdescr[i]))
                                                {
                                                        enqueue_result(resultLine);
                                                        free(resultLine);
                                                }
                                        break;
                                        case 4:/* COMPLETED */
                                                if (resultLine = make_message("%s 1 Job\\ Completed\\ jobId %s", reqId, jobdescr[i]))
                                                {
                                                        enqueue_result(resultLine);
                                                        free(resultLine);
                                                }
                                        break;
                                        case 5:/* HELD */
                                                if(action == RESUME_JOB)
                                                        hold_res_exec(jobdescr[i],reqId,"resume",5);
                                                else
                                                if (resultLine = make_message("%s 0 Job\\ Held\\ jobId %s", reqId, jobdescr[i]))
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
	return;
}

void *
cmd_resume_job(void* args)
{
        hold_resume(args,RESUME_JOB);
        return;
}

void *
cmd_get_hostport(void *args)
{
        char **argv = (char **)args;
        char *reqId = argv[1];
        FILE *cmd_out;
        char *command;
        char *resultLine;
        char hostport[1024];
        int  retcode;
	int i;
	
	if(lrms_counter)
	{
		resultLine = make_message("%s 0 ", reqId);
		for(i =0;i < lrms_counter;i++)
		{        
			//command = make_message("%s/%s_status.sh -n", blah_script_location, server_lrms);
        		command = make_message("%s/%s_status.sh -n", blah_script_location, lrmslist[i]);
			if ((cmd_out=mtsafe_popen(command, "r")) == NULL)
        		{
                		resultLine = make_message("%s 1 Unable\\ to\\ execute\\ %s", reqId,  command );
                		enqueue_result(resultLine);
                		free(command);
                		free_args(argv);
                		return;
        		}
			fgets(hostport, sizeof(hostport), cmd_out);
			if (hostport[strlen(hostport) - 1] == '\n') hostport[strlen(hostport) - 1] = '\0';
        		retcode = mtsafe_pclose(cmd_out);
        		if((!retcode)&&(strlen(hostport) != 0)) 
        			resultLine = make_message("%s%s/%s\ ", resultLine,lrmslist[i], hostport);			
			else
				resultLine = make_message("%s%s/%s\ ", resultLine, lrmslist[i], "Error\\ reading\\ host:port");		
        		free(command);
		}
		enqueue_result(resultLine);
		free(resultLine);
        }

	free_args(argv);
        return ;
}

/* Utility functions
 * */

char 
*escape_spaces(const char *str)
{
	char *buffer;
	int i, j;

	if ((buffer = (char *) malloc (strlen(str) * 2 + 1)) == NULL)
	{
		fprintf(stderr, "Out of memory.\n");
		exit(MALLOC_ERROR);
	}

	for (i = 0, j = 0; i <= strlen(str); i++, j++)
	{
		if (str[i] == ' ') buffer[j++] = '\\';
		buffer[j] = str[i];
	}
	realloc(buffer, strlen(buffer) + 1);
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
	push_result(res, PERSISTENT_BUFFER);
	if (async_notice)
	{
		pthread_mutex_lock(&send_lock);
		write(server_socket, "R\r\n", 3);
		pthread_mutex_unlock(&send_lock);
		/* Don't send it again until a RESULT command is received */
		async_notice = 0;
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

	result = (char *) malloc (n);
	if (result)
		vsnprintf(result, n, fmt, ap);
	va_end(ap);

	return(result);
}

int
limit_proxy(char* proxyname)
{
	char *limcommand;
	char *tmpfilename;
	int res;
	char* globuslocation;
	globuslocation = (getenv("GLOBUS_LOCATION") ? getenv("GLOBUS_LOCATION") : "/opt/globus");
	tmpfilename = make_message("%s.lmt", proxyname);
	limcommand  = make_message("%s/bin/grid-proxy-init -limited -cert %s -key %s -out %s > /dev/null 2>&1",globuslocation, proxyname, proxyname, tmpfilename);
	res = system(limcommand);
	free(limcommand);
	/* If exitcode != 0 there may be a problem due to a warning by grid-proxy-init but */
	/* the call may have been successful. We just check the temporary proxy  */
	if (res)
	{
		limcommand = make_message("%s/bin/grid-proxy-info -f %s > /dev/null 2>&1", globuslocation, tmpfilename);
		res = system(limcommand);
		free(limcommand);
		/* If res is not null, we really got an error */
		if (res) return res;
	}
	if (!res) rename(tmpfilename, proxyname);
	free(tmpfilename);
	return res;
}


int  logAccInfo(char* jobId, char* server_lrms, classad_context cad, char* fqan, char* userDN)
{
        int i=0, rc=0, cs=0, result=0, fd = -1, count = 0, slen = 0, slen2 = 0;
        FILE *cmd_out=NULL;
        FILE *conf_file=NULL;
        FILE *log_file=NULL;
        char *log_line;
        char *proxname=NULL;
        char *gridjobid=NULL;
        char *ce_id=NULL;
        char *login=NULL;
        char *temp_str=NULL;
        char date_str[MAX_TEMP_ARRAY_SIZE], jobid_trunc[MAX_TEMP_ARRAY_SIZE];
        struct flock fl;
        char *AccInfoLogFile=NULL;
        char *AccInfoLogFileDated=NULL;
        time_t tt;
        struct tm *t_m=NULL;
        char *glite_loc=NULL;
        char *blah_conf=NULL;
        char host_name[MAX_CONF_FILE_SIZE];
        char *jobid=NULL;
        char *lrms_jobid=NULL;
        char filebuffer[MAX_CONF_FILE_SIZE];
        int id;
	char bs[4];
	char *queue=NULL;
	char uid[MAX_TEMP_ARRAY_SIZE];
        memset(jobid_trunc,0,MAX_TEMP_ARRAY_SIZE);

        /* Get values from environment and compose the logfile pathname */
        if ((glite_loc = getenv("GLITE_LOCATION")) == NULL)
        {
                glite_loc = DEFAULT_GLITE_LOCATION;
        }
        blah_conf=make_message("%s/etc/blah.config",glite_loc);

        if(blah_conf!=NULL) conf_file= fopen(blah_conf,"r");
        else return 1;

        if(conf_file==NULL)
        {
                free(blah_conf);
                return 1;
        }
        slen=strlen("BLAHPD_ACCOUNTING_INFO_LOG=");
        while(fgets(filebuffer, MAX_CONF_FILE_SIZE, conf_file))
        {
                slen2=strlen(filebuffer);
                if(!strncmp(filebuffer,"BLAHPD_ACCOUNTING_INFO_LOG=",slen))
                {
                        count = slen;
                        while((filebuffer[count]!='\n')&& (count < slen2)) count++;
                        AccInfoLogFile=malloc(count-slen+1);
                        memcpy(AccInfoLogFile, &filebuffer[slen], count-slen);
                        filebuffer[count-slen]=0;
                        AccInfoLogFile[count-slen]=0;
                        slen2=slen=0;
                        break;
                }
                memset(filebuffer,0,MAX_CONF_FILE_SIZE);
        }
        fclose(conf_file);
        /* Submission time */
        time(&tt);
        t_m = gmtime(&tt);
        sprintf(date_str,"%04d-%02d-%02d %02d:%02d:%02d", 1900+t_m->tm_year, t_m->tm_mon+1, t_m->tm_mday, t_m->tm_hour, t_m->tm_min, t_m->tm_sec);

        if(AccInfoLogFile!=NULL)
        {
                AccInfoLogFileDated=make_message("%s-%04d%02d%02d", AccInfoLogFile, 1900+t_m->tm_year, t_m->tm_mon+1, t_m->tm_mday);
                if (AccInfoLogFileDated==NULL)
                {
                        free(AccInfoLogFile);
                        return 1;
                }

                log_file=fopen(AccInfoLogFileDated,"a");
                if (log_file==NULL)
                {
                        free(AccInfoLogFile);
                        free(AccInfoLogFileDated);
                        return 1;
                }
        }
        /* get the lock */
        fd = fileno(log_file);
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_CUR;
        do{
                result = fcntl( fd, F_SETLK, &fl);
        } while((result == -1)&&(errno == EINTR));

        /* These data must be logged in the log file:
         "timestamp=<submission time to LRMS>" "userDN=<user's DN>" "userFQAN=<user's FQAN>"
         "ceID=<CE ID>" "jobID=<grid job ID>" "lrmsID=<LRMS job ID>"
        */
        /* grid jobID  : if we are here we suppose that the edg_jobid is present, if not we log an empty string*/
        if(glexec_mode)
	{
		classad_get_dstring_attribute(cad, "edg_jobid", &gridjobid);
		if(gridjobid==NULL) classad_get_dstring_attribute(cad, "uniquejobid", &gridjobid);
	}else
		classad_get_dstring_attribute(cad, "edg_jobid", &gridjobid);
        if(!gridjobid) gridjobid=make_message("");

        /* job ID */
        strncpy(jobid_trunc, &jobId[JOBID_PREFIX_LEN], strlen(jobId) - JOBID_PREFIX_LEN);
        jobid_trunc[strlen(jobId) - JOBID_PREFIX_LEN]=0;

        /* lrmsID */
        /* No need to add hostname
	jobid=basename(jobid_trunc);
        gethostname(host_name, MAX_CONF_FILE_SIZE);
        slen=strlen(host_name);
        slen2=strlen(jobid);
        if(!strncmp(&jobid[slen2-slen], host_name, slen)) lrms_jobid=strdup(jobid);
        else
                lrms_jobid=make_message("%s.%s",jobid,host_name);
	*/
	lrms_jobid=strdup(jobid);
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
	}
	if(glexec_mode)
	{
	 	/* need to fork and glexec an id command to obtain real user */
		temp_str=make_message("%s /usr/bin/id -u",gloc);
        	if ((cmd_out=mtsafe_popen(temp_str, "r")) == NULL)
                	return 1;
        	fgets(uid, MAX_TEMP_ARRAY_SIZE, cmd_out);
        	mtsafe_pclose(cmd_out);
		free(temp_str);
		uid[strlen(uid)-1]=0;	
	}else
		sprintf(uid,"%d",getuid());
        /* log line with in addiction unixuser */
        log_line=make_message("\"timestamp=%s\" \"userDN=%s\" %s \"ceID=%s\" \"jobID=%s\" \"lrmsID=%s\" \"localUser=%s\"\n",
        date_str, userDN, fqan, ce_id, gridjobid, lrms_jobid, uid);

        cs = fwrite(log_line ,1, strlen(log_line), log_file);
        fl.l_type = F_UNLCK;
        /* release the lock */
        result = fcntl( fd, F_SETLK, &fl);
        fclose(log_file);
	chmod(AccInfoLogFileDated, S_IRUSR|S_IROTH|S_IRGRP|S_IWOTH|S_IWGRP|S_IWUSR);
        if(gridjobid) free(gridjobid);
        free(log_line);
        free(AccInfoLogFile);
        free(AccInfoLogFileDated);
        if (!strcmp(ce_id," ")) free(ce_id);
        memset(fqan,0,MAX_TEMP_ARRAY_SIZE);
	free(jobid);
        free(lrms_jobid);
        return 0;
}

int getProxyInfo(char* proxname, char* fqan, char* userDN)
{
        /* command : openssl x509 -in proxname -subject -noout */
        char temp_str[MAX_TEMP_ARRAY_SIZE];
        FILE *cmd_out=NULL;
        int  result=0;
        int  slen=0;
        int  count=0;
        char fqanlong[MAX_TEMP_ARRAY_SIZE];
	if(glexec_mode) proxname=bssp;
        sprintf(temp_str,"openssl x509 -in %s  -subject -noout", proxname);
        if ((cmd_out=mtsafe_popen(temp_str, "r")) == NULL)
                return 1;
        result = fgets(fqanlong, MAX_TEMP_ARRAY_SIZE, cmd_out);
        result = mtsafe_pclose(cmd_out);
        /* example:
           subject= /C=IT/O=INFN/OU=Personal Certificate/L=Milano/CN=Francesco Prelz/Email=francesco.prelz@mi.infn.it/CN=proxy
           CN=proxy, CN=limited must be elimnated from the bottom of the string
        */
         slen = strlen(fqanlong);
         while(1)
         {
                if (!strncmp(&fqanlong[slen - 10],"/CN=proxy",9))
                {
                      memset(&fqanlong[slen - 10],0,9);
                      slen -=9;
                }else
                if (!strncmp(&fqanlong[slen - 18],"/CN=limited proxy",17))
                {
                      memset(&fqanlong[slen - 18],0,17);
                      slen -=17;
                }else
                          break;
          }
          strcpy(userDN,&fqanlong[9]);
          slen-=9;
          if(userDN[slen - 1] == '/') userDN[slen - 1] = 0;
          /* user'sFQAN detection */
          fqanlong[0]=0;
          memset(fqanlong,0,MAX_TEMP_ARRAY_SIZE);
          /* command : voms-proxy-info -file proxname -fqan  */
	  memset(temp_str,0,MAX_TEMP_ARRAY_SIZE);
	  sprintf(temp_str,"%s/voms-proxy-info -file %s -fqan 2> /dev/null",  blah_script_location, proxname);
          if ((cmd_out=mtsafe_popen(temp_str, "r")) == NULL)
                return 1;
          while(fgets(fqanlong, MAX_TEMP_ARRAY_SIZE, cmd_out))
          {
		strcat(fqan,"\"userFQAN=");
		strcat(fqan,fqanlong);
                memset(fqanlong,0,MAX_TEMP_ARRAY_SIZE);
		if(fqan[strlen(fqan)-1]=='\n') fqan[strlen(fqan)-1] = 0;
		strcat(fqan,"\" ");
          }
	 if (!strcmp(fqan,"")) sprintf(fqan,"\"userFQAN=\"");
	 if(fqan[strlen(fqan)-1]==' ') fqan[strlen(fqan)-1]=0;	
	 result = mtsafe_pclose(cmd_out);
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






