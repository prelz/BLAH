/*
#  File:     resbuffer.c
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#   23 Mar 2004 - Original release
#
#  Description:
#   Mantain the result line buffer
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "blahpd.h"
#include "resbuffer.h"

/* These are global variables since
 * they have to be shared among all threads
 * */
FILE *buffer_file;
static t_resline *first_entry = NULL;
static t_resline *last_entry = NULL;
static int static_num_lines = 0;
static pthread_mutex_t resbuffer_lock  = PTHREAD_MUTEX_INITIALIZER;

/* Init the buffer, load unsent lines from
 * prevoius sessions
 * */
int
init_resbuffer(void)
{
	char resultLine[RESLN_MAX_LEN];
	char *allLines;
	
	pthread_mutex_init(&resbuffer_lock, NULL);

	/* Recover sent but not flushed results */
	if (buffer_file = fopen(FLUSHED_BUFFER, "r"))
	{
		while (fgets(resultLine, RESLN_MAX_LEN, buffer_file))
		{
			while((resultLine[strlen(resultLine)-1] == '\n') || (resultLine[strlen(resultLine)-1] == '\r'))
				resultLine[strlen(resultLine)-1] = '\0';
			push_result(resultLine, BUFFER_DONT_SAVE);
		}
		fclose(buffer_file);
	}
	
	/* Recover unset results */
	if (buffer_file = fopen(BUFFER_FILE, "r"))
	{
		while (fgets(resultLine, RESLN_MAX_LEN, buffer_file))
		{
			while((resultLine[strlen(resultLine)-1] == '\n') || (resultLine[strlen(resultLine)-1] == '\r'))
				resultLine[strlen(resultLine)-1] = '\0';
			push_result(resultLine, BUFFER_DONT_SAVE);
		}
		fclose(buffer_file);
	}

	/* Make current buffer persistent */
	if (allLines = get_lines(BUFFER_DONT_FLUSH))
	{
		if (buffer_file = fopen(BUFFER_FILE, "w"))
		{
			fprintf(buffer_file, "%s\n", allLines);
			fclose(buffer_file);
		}
		free(allLines);
	}

	remove(FLUSHED_BUFFER);
}
	
/* Clean up the result buffer
 * */
int
flush_buffer(void)
{
	while(first_entry)
	{
		last_entry = first_entry->next;
		free(first_entry->text);
		free(first_entry);
		first_entry = last_entry;
	}
	static_num_lines = 0;
	rename(BUFFER_FILE, FLUSHED_BUFFER);
}

/* Push a new result line into result buffer
 * */
int
push_result(const char *res, const int save)
{
	FILE *buffer_file;
	t_resline *new_entry;

	/* fprintf(stderr, "DEBUG: pushing '%s'\n", res); */
	/* Allocate and init a new result line
	 * */
	if ((new_entry = (t_resline *) malloc (sizeof(t_resline))) == NULL)
		return(MALLOC_ERROR);
	new_entry->next = NULL;
	if ((new_entry->text = (char *) malloc (strlen(res) + 1)) == NULL)
		return(MALLOC_ERROR);
	strcpy(new_entry->text, res);

	/* Don't let other threads modify the pointers
	 * while adding the new entry to the list
	 * */
	pthread_mutex_lock(&resbuffer_lock);
	/* Add the new entry to the list
	 * */
	if (!first_entry) first_entry = new_entry;
	if (last_entry) last_entry->next = new_entry;
	last_entry = new_entry;
	static_num_lines++;
	if (save)
	{
		if (buffer_file = fopen(BUFFER_FILE, "a"))
		{
			fprintf(buffer_file, "%s\n", res);
			fclose(buffer_file);
		}
	}
	/* Green light for other threads
	 * */
	pthread_mutex_unlock(&resbuffer_lock);
}

int
num_results(void)
{
	int result;
	
	pthread_mutex_lock(&resbuffer_lock);
	result = static_num_lines;
	pthread_mutex_unlock(&resbuffer_lock);
	return(result);
}

/* Join all the result lines in a single string
 * and flush the buffer
 * */

char *
get_lines(const int flush_bf)
{
	char *res_lines = NULL;
	char *reallocated;
	t_resline *line;
	int last_size = 0;

	/* Don't let other threads modify the pointers
	 * while we are reading or flushing the buffer
	 * */
	pthread_mutex_lock(&resbuffer_lock);
	for(line = first_entry; line != NULL; line = line->next)
	{
		reallocated = (char *) realloc (res_lines, last_size + strlen(line->text)+ strlen(BLAHPD_CRLF) + 1);
		if (!reallocated)
		{
			if (res_lines) free (res_lines);
			return(NULL);
		}
		else
			res_lines = reallocated;
		if (line != first_entry)
			strcat(res_lines, BLAHPD_CRLF);
		else
			res_lines[0] = '\000';
		strcat(res_lines, line->text);
		last_size += strlen(res_lines);
	}
	if (flush_bf) flush_buffer();
	/* Green light for other threads
	 * */
	pthread_mutex_unlock(&resbuffer_lock);
	return(res_lines);
}

