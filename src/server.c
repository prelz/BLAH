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

#include "blahpd.h"
#include "classad_c_helper.h"
#include "commands.h"
#include "job_status.h"
#include "resbuffer.h"

#define COMMAND_PREFIX "-c"
#define PERSISTENT_BUFFER BUFFER_DONT_SAVE

t_resline *first_job = NULL;
t_resline *last_job = NULL;
int num_jobs = 0;

#define NO_QUOTE     0
#define SINGLE_QUOTE 1
#define DOUBLE_QUOTE 2

#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE  1
#endif

const char *opt_format[3] = {
	" %s %s",               /* NO_QUOTE */
	" %s \"%s\"",           /* SINGLE_QUOTE */
	" %s \"\\\"%s\\\"\""    /* DOUBLE_QUOTE */
};

/* Function prototypes */
char *get_command(int client_socket);
char *escape_spaces(const char *str);
int set_cmd_list_option(char **command, classad_context cad, const char *attribute, const char *option);
int set_cmd_string_option(char **command, classad_context cad, const char *attribute, const char *option, const int quote_style);

/* Global variables */
static int server_socket;
static int async_mode = 0;
static int async_notice = 0;
static int exit_program = 0;
static pthread_mutex_t send_lock  = PTHREAD_MUTEX_INITIALIZER;
char *server_lrms;
char *blah_script_location;
char *blah_version;


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

	init_resbuffer();
	if (cli_socket == 0) server_socket = 1;
	else                 server_socket = cli_socket;

	/* Get values from environment */
	if ((result = getenv("GLITE_LOCATION")) == NULL)
	{
		result = DEFAULT_GLITE_LOCATION;
	}
	blah_script_location = make_message(BINDIR_LOCATION, result);
	if ((server_lrms = getenv("BLAH_LRMS")) == NULL)
	{
		server_lrms = DEFAULT_LRMS;
	}
	blah_version = make_message(RCSID_VERSION, VERSION, server_lrms);

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

	if (result = (char *) malloc (13)) /* hope 10 digits suffice*/
	{
		sprintf(result, "%d", num_results());
		if(num_results())
		{
			strcat(result, "\r\n");
			res_lines = get_lines(BUFFER_FLUSH);
			result = (char *)realloc(result, strlen(result) + strlen(res_lines) + 2); /* CHECK THIS */
			strcat(result, res_lines);
			free(res_lines);
		}
	}
	
	/* From now send 'R' when a new resline is enqueued */
	async_notice = async_mode;
	
	/* If malloc has failed, return NULL to notify error */
	return(result);
}

/* Threaded commands
 * must free argv before return
 * */

void *
cmd_submit_job(void *args)
{
	const char *submission_cmd;
	FILE *cmd_out;
	int retcod;
	char *command;
	char jobId[JOBID_MAX_LEN];
	char resultLine[RESLN_MAX_LEN];
	char **argv = (char **)args;
	char **arg_ptr;
	classad_context cad;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	int r;

	cad = classad_parse(jobDescr);
	if (cad == NULL)
	{
		/* PUSH A FAILURE */
		sprintf(resultLine, "%s 1 Error\\ parsing\\ classad N/A", reqId);
		enqueue_result(resultLine);
		return;
	}

	command = make_message("%s/%s_submit.sh", blah_script_location, server_lrms);
	if (command == NULL)
	{
		/* PUSH A FAILURE */
		sprintf(resultLine, "%s 1 Out\\ of\\ Memory N/A", reqId);
		enqueue_result(resultLine);
		return;
	}

	/* Cmd attribute is mandatory: stop on any error */
	if (set_cmd_string_option(&command, cad, "Cmd", COMMAND_PREFIX, NO_QUOTE) != C_CLASSAD_NO_ERROR)
	{
		/* PUSH A FAILURE */
		sprintf(resultLine, "%s 7 Cannot\\ parse\\ Cmd\\ attribute\\ in\\ classad N/A", reqId);
		enqueue_result(resultLine);
		return;
	}
	
	/* All other attributes are optional: fail only on memory error */
	if ((set_cmd_string_option(&command, cad, "In",       "-i", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Out",      "-o", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Err",      "-e", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Iwd",      "-w", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Env",      "-v", SINGLE_QUOTE)  == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Args",     "--", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_string_option(&command, cad, "Queue",    "-q", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY) ||
	    (set_cmd_bool_option  (&command, cad, "StageCmd", "-s", NO_QUOTE)      == C_CLASSAD_OUT_OF_MEMORY))
	{
		/* PUSH A FAILURE */
		sprintf(resultLine, "%s 1 Out\\ of\\ memory\\ parsing\\ classad N/A", reqId);
		enqueue_result(resultLine);
		return;
	}

	fprintf(stderr, "DEBUG: submission cmd = '%s'\n", command);
	if((cmd_out = popen(command, "r")) == NULL)
	{
		/* PUSH A FAILURE */
		sprintf(resultLine, "%s 3 Unable\\ to\\ open\\ pipe\\ for\\ submit N/A", reqId);
		enqueue_result(resultLine);
		return;
	}
	fgets(jobId, sizeof(jobId), cmd_out);
	if (jobId[strlen(jobId) - 1] == '\n') jobId[strlen(jobId) - 1] = '\000';
	retcod = pclose(cmd_out);
	if (retcod != 0)
	{
		/* PUSH A FAILURE */
		sprintf(resultLine, "%s 2 Submit\\ command\\ exit\\ with\\ retcode\\ %d N/A", reqId, retcod);
		enqueue_result(resultLine);
		return;
	}
	/* PUSH A SUCCESS */
	sprintf(resultLine, "%s 0 No\\ error %s", reqId, jobId);
	enqueue_result(resultLine);

	/* Free up all arguments */
	classad_free(cad);
	free(command);
	free_args(argv);

	return;
}

void *
cmd_cancel_job(void* args)
{
	int retcod;
	char *command;
	char *resultLine = NULL;
	char **argv = (char **)args;
	char **arg_ptr;

	char *reqId = argv[1];
	char *jobDescr = argv[2];

	command = make_message("%s/%s_cancel.sh %s", blah_script_location, server_lrms, jobDescr);
	if (command == NULL)
	{
		/* PUSH A FAILURE */
		if (resultLine = make_message("%s 1 Out\\ of\\ Memory", reqId))
		{	
			enqueue_result(resultLine);
			free(resultLine);
		}
		return;
	}

	fprintf(stderr, "DEBUG: executing %s\n", command);
	retcod = system(command);
	if (resultLine = make_message("%s %d %s", reqId, retcod, retcod ? "Error" : "No\\ error")) {
		enqueue_result(resultLine);
		free (resultLine);
	}
	free(command);
	free_args(argv);

	return;
}

void *
cmd_status_job(void *args)
{
	classad_context status_ad;
	char *str_cad;
	char *esc_str_cad;
	char *resultLine;
	char **argv = (char **)args;
	char **arg_ptr;
	char errstr[ERROR_MAX_LEN] = "No error";
	char *esc_errstr;
	char *reqId = argv[1];
	char *jobDescr = argv[2];
	int jobStatus, retcode;

	retcode = get_status(jobDescr, &status_ad, errstr);
	if (esc_errstr = escape_spaces(errstr))
	{
		if (!retcode)
		{
			classad_get_int_attribute(status_ad, "JobStatus", &jobStatus);
			str_cad = classad_unparse(status_ad);
			esc_str_cad = escape_spaces(str_cad);
			resultLine = make_message("%s %d %s %d %s", reqId, retcode, esc_errstr, jobStatus, esc_str_cad);
			free(str_cad);
			free(esc_str_cad);
		}
		else
		{
			resultLine = make_message("%s %d %s 0 N/A", reqId, retcode, esc_errstr);
		}

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
	}
	else
		enqueue_result("Missing result line due to memory error");
	
	/* Free up all arguments */
	classad_free(status_ad);
	free_args(argv);
	
	return;
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
	int allocated_size = 0;
	char buffer[256];
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
					message = (char *) realloc (message, allocated_size);
					if (message == NULL)
					{
						allocated_size = 0;
						perror("Error allocating buffer for incoming message");
						close(s);
						exit(MALLOC_ERROR);
					}
				}
				memcpy(&message[read_chars],buffer,recv_chars);
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
		else
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

