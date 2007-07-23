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
#include "blahpd.h"

static pthread_mutex_t poperations_lock  = PTHREAD_MUTEX_INITIALIZER;
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



/* This function forks a new process to run a command, and returns
   the exit code and the stdout in an allocated buffer.
   The caller must take care of freing the buffer.
*/

#define BUFFERSIZE 1024
#define MAX_ENV_SIZE 8192

int
exe_getout(char *const command, char *const environment[], char **cmd_output)
{
	int fdpipe[2];
	int pid;
	int status, exitcode;
	char **envcopy = NULL;
	int envcopy_size;
	int i = 0, char_count, res_len = 0;
	char buffer[BUFFERSIZE];
	char *killed_format = "killed by signal %02d";
	wordexp_t args;

	*cmd_output = NULL;

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

	if (pipe(fdpipe) == -1)
	{
		perror("pipe");
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
			/* Connect stdout to the pipe */
			if (dup2(fdpipe[1], 1) == -1)
			{
				perror("dup2");
				exit(1);       
			}

			/* Close unused pipes */
			close(fdpipe[0]);
			close(fdpipe[1]);

			/* Execute the command */
			execve(args.we_wordv[0], args.we_wordv, envcopy);
			exit(errno);

		default: /* Parent process */
			/* Close unused pipe */
			close(fdpipe[1]);

			/* Free the copy of the environment */
			for (i = 0; envcopy[i] != NULL; i++)
				free(envcopy[i]);
			free(envcopy);

			/* Free the wordexp'd args */
			wordfree(&args);

			/* Wait for the command to finish */
			waitpid(pid, &status, 0);

			if (WIFEXITED(status))
			{
				/* Initialise empty result */
				if ((*cmd_output = (char *)malloc(sizeof(char))) == NULL)
				{
					fprintf(stderr, "out of memory!\n");
					exit(1);
				}
				*cmd_output[0] = '\000';

				/* Read the command's output */
				while((char_count = read(fdpipe[0], buffer, sizeof(buffer) - 1)) > 0)
				{
					buffer[char_count] = '\000';
					if ((*cmd_output = (char *)realloc(*cmd_output, res_len + char_count + 1)) == NULL)
					{
						fprintf(stderr, "out of memory!\n");
						exit(1);
					}
					strcpy(*cmd_output + res_len, buffer);
					res_len += char_count;
				}

				/* Close the pipe */
				close(fdpipe[0]);
				exitcode = WEXITSTATUS(status);
			}
			else if (WIFSIGNALED(status))
			{
				exitcode = WTERMSIG(status);
#ifdef _GNU_SOURCE
				*cmd_output = strdup(strsignal(exitcode));
#else
				/* FIXME: should import make_message here too */
				*cmd_output = (char *)malloc(strlen(killed_format));
				snprintf(cmd_output, strlen(killed_format), killed_format, exitcode);
#endif
				exitcode = -exitcode;
			}
			else
			{
				fprintf(stderr, "Child process terminated abnormally\n");
				exit(1);
			}
			return(exitcode);	
	}
}

