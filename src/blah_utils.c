/*
#  File:     blah_utils.c
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#   30 Mar 2009 - Original release.
#
#  Description:
#   Utility functions for blah protocol
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include "blah_utils.h"

const char *blah_omem_msg = "out\\ of\\ memory";

char *
make_message(const char *fmt, ...)
{
	/* Dynamically allocate a string of proper length
	   and initialize it with the provided parameters. 
	*/
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

char *
escape_spaces(const char *str)
{
	/* Makes message strings compatible with blah protocol:
	   escape white spaces with a backslash,
	   replace tabs with spaces, CR and LF with '-'.
	*/
	char *result = NULL;
	char cur;
	int i, j;

	result = (char *) malloc (strlen(str) * 2 + 1);
	if (result)
	{
		for (i = 0, j = 0; i <= strlen(str); i++, j++)
		{
			cur = str[i];
			if (cur == '\r') cur = '-';
			else if (cur == '\n') cur = '-';
			else if (cur == '\t') cur = ' ';

			if (cur == ' ') result[j++] = '\\';
			result[j] = cur;
		}
	}
	else
		result = (char *)blah_omem_msg;
	return(result);
}

