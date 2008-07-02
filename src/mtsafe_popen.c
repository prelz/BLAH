/*
#  File:     mtsafe_popen.c
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#    7 Sep 2005 - Original release
#
#  Description:
#   Implements a mutexed popen a pclose to be MT safe
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <wordexp.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/poll.h>
#include "blahpd.h"

static pthread_mutex_t poperations_lock  = PTHREAD_MUTEX_INITIALIZER;
extern config_handle *blah_config_handle;
extern char **environ;

int
init_poperations_lock(void)
{
	pthread_mutex_init(&poperations_lock, NULL);
}

FILE *
mtsafe_popen(const char *command, const char *type)
{
	FILE *result;

#ifdef MT_DEBUG
	int retcode;

	retcode = pthread_mutex_trylock(&poperations_lock);
	if (retcode)
	{
		fprintf(stderr, "Thread %d: another thread was popening\n", pthread_self());
#endif
	pthread_mutex_lock(&poperations_lock);
#ifdef MT_DEBUG
	}
#endif
	result = popen(command, type);
	pthread_mutex_unlock(&poperations_lock);
	return(result);
}

int
mtsafe_pclose(FILE *stream)
{
	int result;

#ifdef MT_DEBUG
	int retcode;

	retcode = pthread_mutex_trylock(&poperations_lock);
	if (retcode)
	{
		fprintf(stderr, "Thread %d: another thread was pclosing\n", pthread_self());
#endif
	pthread_mutex_lock(&poperations_lock);
#ifdef MT_DEBUG
	}
#endif
	result = pclose(stream);
	pthread_mutex_unlock(&poperations_lock);
	return(result);
}



/* Utility functions */
int
merciful_kill(pid_t pid)
{
	int graceful_timeout = 10; /* Default value - overridden by config */
	int status=-1;
	config_entry *config_timeout;
	int tmp_timeout;
	int tsl;

	if (blah_config_handle != NULL && 
	    (config_timeout=config_get("blah_graceful_kill_timeout",blah_config_handle)) != NULL)
	{
		tmp_timeout = atoi(config_timeout->value);
		if (tmp_timeout > 0) graceful_timeout = tmp_timeout;
	}

	if (waitpid(pid, &status, WNOHANG) == 0)
	{
		if (kill(pid, SIGTERM) == 0)
		{
			/* verify that child is dead */
			for(tsl = 0; (waitpid(pid, &status, WNOHANG) == 0) &&
			              tsl < graceful_timeout; tsl++)
			{
				/* still alive, allow a few seconds 
				   than use brute force */
				sleep(1);
			}
			if ((waitpid(pid, &status, WNOHANG) == 0) && tsl >= graceful_timeout)
			{
				if (kill(pid, SIGKILL) == 0)
				{
					waitpid(pid, &status, 0);
				}
			}

		}
	}
	return(status);
}

int
read_data(const int fdpipe, char **buffer)
{
	char local_buffer[1024];
	int char_count;
	int i;

	if (*buffer == NULL) return(-1);

	if ((char_count = read(fdpipe, local_buffer, sizeof(local_buffer) - 1)) > 0)
	{
		if ((*buffer = (char *)realloc(*buffer, strlen(*buffer) + char_count + 1)) == NULL)
		{
			fprintf(stderr, "out of memory!\n");
			exit(1);
		}
		local_buffer[char_count] = '\000';
		/* Any stray NUL in the output string ? */
		for (i=0; i<char_count; i++) if (local_buffer[i] == '\000') local_buffer[i]='.';
		strcat(*buffer, local_buffer);
	}
	return(char_count);
}

/* This function forks a new process to run a command, and returns
   the exit code and the stdout in an allocated buffer.
   The caller must take care of freing the buffer.
*/

#define MAX_ENV_SIZE 8192
#define BUFFERSIZE 2048

int
exe_getouterr(char *const command, char *const environment[], char **cmd_output, char **cmd_error)
{
	int fdpipe_stdout[2];
	int fdpipe_stderr[2];
	struct pollfd pipe_poll[2];
	int poll_timeout = 30000; /* 30 seconds by default */
	pid_t pid;
	int child_running, status, exitcode;
	char **envcopy = NULL;
	int envcopy_size;
	int i = 0;
	char buffer[BUFFERSIZE];
	char *killed_msg;
	char *killed_format = "killed by signal %02d\n";
	char *new_cmd_error;
	wordexp_t args;
	config_entry *config_timeout;
	int tmp_timeout;
	int successful_read;

	if (blah_config_handle != NULL && 
	    (config_timeout=config_get("blah_child_poll_timeout",blah_config_handle)) != NULL)
	{
		tmp_timeout = atoi(config_timeout->value);
		if (tmp_timeout > 0) poll_timeout = tmp_timeout*1000;
	}

	if (cmd_error == NULL || cmd_output == NULL) return(-1);

	*cmd_output = NULL;
	*cmd_error = NULL;

	/* Copy original environment */
	for (envcopy_size = 0; environ[envcopy_size] != NULL; envcopy_size++)
	{
		envcopy = (char **)realloc(envcopy, sizeof(char *) * (envcopy_size + 1));
		envcopy[envcopy_size] = strdup(environ[envcopy_size]);
	}

	/* Add the required environment */
	if (environment)
		for(i = 0; environment[i] != NULL; i++)
		{
			envcopy = (char **)realloc(envcopy, sizeof(char *) * (envcopy_size + i + 1));
			envcopy[envcopy_size + i] = strdup(environment[i]);
		}

	/* Add the NULL terminator */
	envcopy = (char **)realloc(envcopy, sizeof(char *) * (envcopy_size + i + 1));
	envcopy[envcopy_size + i] = (char *)NULL;

	/* Do the shell expansion */
	if(i = wordexp(command, &args, 0))
	{
		fprintf(stderr,"wordexp: unable to parse the command line \"%s\" (error %d)\n", command, i);
		return(1);
	}

#ifdef EXE_GETOUT_DEBUG
	fprintf(stderr, "DEBUG: blahpd invoking the command '%s'\n", command);
#endif

	if (pipe(fdpipe_stdout) == -1)
	{
		perror("pipe() for stdout");
		return(-1);       
	}
	if (pipe(fdpipe_stderr) == -1)
	{
		perror("pipe() for stderr");
		return(-1);       
	}

	switch(pid = fork())
	{
		case -1:
			perror("fork");
			return(-1);

		case 0: /* Child process */
			/* CAUTION: fork was invoked from within a thread!
			 * Do NOT use any fork-unsafe function! */
			/* Connect stdout & stderr to the pipes */
			if (dup2(fdpipe_stdout[1], STDOUT_FILENO) == -1)
			{
				perror("dup2() stdout");
				exit(1);       
			}
			if (dup2(fdpipe_stderr[1], STDERR_FILENO) == -1)
			{
				perror("dup2() stderr");
				exit(1);       
			}

			/* Close unused pipes */
			close(fdpipe_stdout[0]);
			close(fdpipe_stdout[1]);
			close(fdpipe_stderr[0]);
			close(fdpipe_stderr[1]);

			/* Execute the command */
			execve(args.we_wordv[0], args.we_wordv, envcopy);
			exit(errno);

		default: /* Parent process */
			/* Close unused pipes */
			close(fdpipe_stdout[1]);
			close(fdpipe_stderr[1]);

			/* Free the copy of the environment */
			for (i = 0; envcopy[i] != NULL; i++)
				free(envcopy[i]);
			free(envcopy);

			/* Free the wordexp'd args */
			wordfree(&args);

			/* Initialise empty stderr and stdout */
			if ((*cmd_output = (char *)malloc(sizeof(char))) == NULL ||
			    (*cmd_error = (char *)malloc(sizeof(char))) == NULL)
			{
				fprintf(stderr, "out of memory!\n");
				exit(1);
			}
			*cmd_output[0] = '\000';
			*cmd_error[0] = '\000';


			child_running = 1;
			while(child_running)
			{
				/* Initialize fdpoll structures */
				pipe_poll[0].fd = fdpipe_stdout[0];
				pipe_poll[0].events = ( POLLIN | POLLERR | POLLHUP | POLLNVAL );
				pipe_poll[0].revents = 0;
				pipe_poll[1].fd = fdpipe_stderr[0];
				pipe_poll[1].events = ( POLLIN | POLLERR | POLLHUP | POLLNVAL );
				pipe_poll[1].revents = 0;
				switch(poll(pipe_poll, 2, poll_timeout))
				{
					case -1: /* poll error */
						perror("poll()");
						exit(1);
					case 0: /* timeout occurred */
						/* kill the child process */
						status = merciful_kill(pid);
						child_running = 0;
						break;
					default: /* some event occurred */
						successful_read = 0;
						if (pipe_poll[0].revents & POLLIN)
						{
							if (read_data(pipe_poll[0].fd, cmd_output)>=0)
								successful_read = 1;
						}
						if (pipe_poll[1].revents & POLLIN)
						{
							if (read_data(pipe_poll[1].fd, cmd_error)>=0)
								successful_read = 1;
						}
						if (successful_read == 0)
						{
							status = merciful_kill(pid);
							child_running = 0;
							break;
						}
				}
			}

			close(fdpipe_stdout[0]);
			close(fdpipe_stderr[0]);

			if (WIFEXITED(status))
			{
				exitcode = WEXITSTATUS(status);
			}
			else if (WIFSIGNALED(status))
			{
				exitcode = WTERMSIG(status);
#ifdef _GNU_SOURCE
				killed_msg = strdup(strsignal(exitcode));
#else
				/* FIXME: should import make_message here too */
				killed_msg = (char *)malloc(strlen(killed_format));
				if (killed_msg != NULL)
					snprintf(killed_msg, strlen(killed_format), killed_format, exitcode);
#endif
				exitcode = -exitcode;
				if (killed_msg != NULL)
				{
					/* Append it to the stderr */
					new_cmd_error = (char *)realloc(*cmd_error, strlen(*cmd_error)+strlen(killed_msg)+2);
					if (new_cmd_error != NULL)
					{
						strcat(new_cmd_error,"\n");
						strcat(new_cmd_error,killed_msg);
						*cmd_error = new_cmd_error;
					}
				}
			}
			else
			{
				fprintf(stderr, "exe_getout: Child process terminated abnormally\n");
				exit(1);
			}
			return(exitcode);	
	}
}

int
exe_getout(char *const command, char *const environment[], char **cmd_output)
{
	int result;
	char *cmd_error;

	result = exe_getouterr(command, environment, cmd_output, &cmd_error);
	if (cmd_error)
	{
		if (*cmd_error != '\000')
			fprintf(stderr, "blahpd:exe_getouterr() stderr caught for command <%s>:\n%s\n", command, cmd_error);
		free (cmd_error);
	}
	return(result);
}

