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

static pthread_mutex_t poperations_lock  = PTHREAD_MUTEX_INITIALIZER;

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
