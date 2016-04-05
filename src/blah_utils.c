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
#  Copyright (c) Members of the EGEE Collaboration. 2007-2010. 
#
#    See http://www.eu-egee.org/partners/ for details on the copyright
#    holders.  
#  
#    Licensed under the Apache License, Version 2.0 (the "License"); 
#    you may not use this file except in compliance with the License. 
#    You may obtain a copy of the License at 
#  
#        http://www.apache.org/licenses/LICENSE-2.0 
#  
#    Unless required by applicable law or agreed to in writing, software 
#    distributed under the License is distributed on an "AS IS" BASIS, 
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#    See the License for the specific language governing permissions and 
#    limitations under the License.
#
*/

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include "blah_utils.h"

const char *blah_omem_msg = "out\\ of\\ memory";

/* 
 * make message
 *
 * Dynamically allocate a string of proper length
 * and initialize it with the provided parameters
 * N.B. glibc 2.1 needed.
 *
 * @param fmt   Specifies the format to apply (see snprintf(3))
 * @param ...   The arguments to be formatted
 *
 * @return      Dynamically allocated string containing the formatting
 *              result. Needs to be free'd.
 */
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

/*
 * escape_espaces
 *
 * Makes message strings compatible with blah protocol:
 * escape white spaces and backslashes with a backslash,
 * replace tabs with (escaped) spaces, replace CR and LF 
 * with '-'.
 *
 * @param str  The string to convert
 *
 * @return     Dynamically allocated string containing the formatting
 *             result. Needs to be free'd.
 */
char *
escape_spaces(const char *str)
{
	char *result = NULL;
	const char *cur;
	char *dest;
	size_t new_len = 0;

	if (str == NULL)
	{
		errno = EINVAL;
		return NULL;
	}

	/* At the cost of iterating one more time on the string we can predict
	 * the real length. Actually we spare a call to strlen(), which also
	 * iterates on the string...    */
	for (cur = str; *cur != '\000'; ++cur)
		if (*cur == ' ' || *cur == '\t' || *cur == '\\') new_len++;
	new_len += (cur - str);

	result = (char *) malloc (sizeof (char) * new_len + 1);
	if (result)
	{
		cur = str;
		dest = result;
		do
		{
			switch (*cur)
			{
			/* replaced chars */
			case '\r':
			case '\n':
				*(dest++) = '-';
				break;
			/* replaced and escaped chars */
			case '\t':
				*(dest++) = '\\';
				*(dest++) = ' ';
				break;
			/* just escaped chars */
			case ' ':
			case '\\':
				*(dest++) = '\\';
				/* intentionally no break */
			default:
				*(dest++) = *cur;
			}
		} while (*(cur++) != '\000');
	}
	else
		result = (char *)blah_omem_msg;
	return(result);
}

/*
 * convert_newstyle
 *
 * Convert arguments and environment strings formatted according to 
 * Condor "new syntax" into a single string to be passed on the command
 * line. Each element of the original string is enclosed in double quotes
 * and separated by the caller specified 'separator' char.
 * Quotes, double quotes and special chars are escaped according to bash rules. 
 * 
 * @param original   The original string as extracted from the job classad
 * @param separator  The character to use as separator
 *
 * @return           Dynamically allocated string containing the formatting
 *                   result. Needs to be free'd.
 *
 */

#define SINGLE_QUOTE_CHAR '\''
#define DOUBLE_QUOTE_CHAR '\"'
#define CONVARG_OPENING        "'\""
#define CONVARG_OPENING_LEN    2
#define CONVARG_CLOSING        "\"'\000"
#define CONVARG_CLOSING_LEN    3
#define CONVARG_QUOTSEP        "\"%c\""
#define CONVARG_QUOTSEP_LEN    3
#define CONVARG_DBLQUOTESC     "\\\\\\\""
#define CONVARG_DBLQUOTESC_LEN 4
#define CONVARG_SNGQUOTESC     "'\\''"
#define CONVARG_SNGQUOTESC_LEN 4
/* set this to the length of the longest escape sequence */
#define CONVARG_OVERCOMMIT     4

char *
convert_newstyle(const char* original, const char separator)
{

	int inside_quotes = 0;
	char *result;
	char *tmp_realloc;
	int i, j;
	size_t orig_len;
	size_t max_len;
	char quoted_sep[CONVARG_QUOTSEP_LEN];

	orig_len = strlen(original);
	if (orig_len == 0)
	{
		errno = EINVAL;
		return NULL;
	}

	sprintf(quoted_sep, CONVARG_QUOTSEP, separator);

	/* assume longest escape sequence for all chars in original string */
	max_len = orig_len * CONVARG_OVERCOMMIT;
	result = (char *)malloc(max_len);
	if (result == NULL)
	{
		errno = ENOMEM;
		return NULL;
	}

	memcpy(result, CONVARG_OPENING, CONVARG_OPENING_LEN);
	j = CONVARG_OPENING_LEN;

	for(i=0; i < orig_len; i++)
	{
		/* were we too optimistic with overcommit? */
		if (j > max_len - CONVARG_OVERCOMMIT)
		{
			if (tmp_realloc = (char *)realloc(result, 
			    max_len + orig_len))
			{
				result = tmp_realloc;
				max_len += orig_len;
			} else {
				free(result);	
				errno = ENOMEM;
				return NULL;
			}
		}

		/* examine the current character */
		switch (original[i])
		{
		case SINGLE_QUOTE_CHAR:
			if (!inside_quotes)
			{	/* opening quote, don't copy it */
				inside_quotes = 1;
			}
			else if (original[i+1] == SINGLE_QUOTE_CHAR) 
			{	/* two quotes: is a literal quote */
				memcpy(result + j, CONVARG_SNGQUOTESC, 
				       CONVARG_SNGQUOTESC_LEN);
				j += CONVARG_SNGQUOTESC_LEN;
				++i;
			}
			else
			{	/* closing quote, don't copy it */
				inside_quotes = 0;
			}
			break;
		case ' ':
			if (inside_quotes)
			{	/* the blank is a literal, copy */
				result[j++] = original[i];
			}
			else
			{	/* the blank is a separator */
				memcpy(result + j, quoted_sep,
				       CONVARG_QUOTSEP_LEN);
				j += CONVARG_QUOTSEP_LEN;
			}
			break;
		case DOUBLE_QUOTE_CHAR:
			/* double quotes need to be triple-escaped to make it
			   to the submit file */
			memcpy(result + j, CONVARG_DBLQUOTESC,
			       CONVARG_DBLQUOTESC_LEN);
			j += CONVARG_DBLQUOTESC_LEN;
			break;
#if 0
		/* Must escape a few meta-characters for wordexp */
			/* Not really needed: the result string is enclosed in single
			 * quotes so it is not susceptible to any expansion.
			 * Left as example if we ever need to escape some chars.
			 * DR 4 Apr 2016 */
		case '(':
		case ')':
		case '|':
		case '$':
			result[j++] = '\\';
			result[j++] = original[i];
			break;
#endif
		default:
			/* plain copy from the original */
			result[j++] = original[i];
		} /* switch */
	} /* for */

	if (inside_quotes)
	{	/* bad string: unmatched quote */
		free(result);
		errno = EINVAL;
		return NULL;
	}

	memcpy(result + j, CONVARG_CLOSING, CONVARG_CLOSING_LEN);
	return(result);
}
