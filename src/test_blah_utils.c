/*
 *  File :     test_blah_utils.c
 *
 *
 *  Author :   David Rebatto
 *  e-mail :   "david.rebatto@mi.infn.it"
 *
 *  Revision history :
 *  5-Apr-2016 Original release
 *
 *  Description:
 *   Test functions in blah_utils.c.
 *
 *  Copyright (c) Members of the EGEE Collaboration. 2007-2010. 
 *
 *    See http://www.eu-egee.org/partners/ for details on the copyright
 *    holders.  
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License"); 
 *    you may not use this file except in compliance with the License. 
 *    You may obtain a copy of the License at 
 *  
 *        http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *    Unless required by applicable law or agreed to in writing, software 
 *    distributed under the License is distributed on an "AS IS" BASIS, 
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *    See the License for the specific language governing permissions and 
 *    limitations under the License.
 */


#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include "blah_utils.h"

typedef struct {
	const char *orig;
	const char *expected;
	const int error;
} test_case_t;

int
perform_test(char *(*test_fn)(const char *), test_case_t test_cases[], const int verbose)
{

	int i;
	char *result;

	for(i = 0; test_cases[i].orig != NULL; ++i)
	{
		if (verbose) printf("ORIG: [%s]\n", test_cases[i].orig);
		if (verbose) printf("EXP:  [%s]\n", test_cases[i].expected);
		if ((result = (*test_fn)(test_cases[i].orig)) != NULL)
		{
			if (verbose) printf("RES:  [%s]\n", result);
			assert(strcmp(result, test_cases[i].expected) == 0);
			free(result);
		} else {
			if (verbose) printf("convert_newstyle(): %s\n", strerror(errno));
			assert(test_cases[i].error == errno);
		}
		if (verbose) printf("\n");
	}
	return 0;
}

/* Wrap convert_newstyle in a single parameter function */
char *
test_newstyle(const char *s)
{
	return convert_newstyle(s, ' ');
}

int
main(int argc, char *argv[])
{

	test_case_t newstyle_cases[] =
	{
		/* Environment */
		{ "QUOTEDVALUE='''VALUE 0''' SIMPLEVALUE=VALUE0",
		  "'\"QUOTEDVALUE='\\''VALUE 0'\\''\" \"SIMPLEVALUE=VALUE0\"'",
		  0 },
		{ "DOUBLEQUOTEDVALUE='\"Foo bar\"' DBLQUOTECHAR=\"",
		  "'\"DOUBLEQUOTEDVALUE=\\\\\\\"Foo bar\\\\\\\"\" \"DBLQUOTECHAR=\\\\\\\"\"'",
		  0 },
		{ "VALUEWITHSPACES1='a b c'",
		  "'\"VALUEWITHSPACES1=a b c\"'",
		  0 },
		{ "VALUEWITHSPACES2=a' 'b' 'c",
		  "'\"VALUEWITHSPACES2=a b c\"'",
		  0 },

		/* Arguments */
		{ "\"a\" ''''b'''' c' 'd e' 'f ''''", /* Jaime's example from 2006 */
		  "'\"\\\\\\\"a\\\\\\\"\" \"'\\''b'\\''\" \"c d\" \"e f\" \"'\\''\"'",
		  0 },
		{ "a b ",
		  "'\"a\" \"b\" \"\"'",
		  0 },

		/* Bad cases, for sake of completeness. The arguments come from classad 
		   unparsing via classad library, so they should never be malformed */
		{ "", /* empty string not admitted */
		  NULL,
		  EINVAL },
		{ "BADVALUE='a b c", /* unmatched quote */
		  NULL,
		  EINVAL },

		/* terminate */
		{ NULL, NULL, 0 }
	};

	test_case_t escape_spaces_cases[] =
	{
		{ "Example message\non two lines\n",
		  "Example\\ message-on\\ two\\ lines-",
		  0 },
		{ "Here's a tab <\t> and a backslash <\\>",
		  "Here's\\ a\\ tab\\ <\\ >\\ and\\ a\\ backslash\\ <\\\\>",
		  0 },
		{ "", /* zero lenght string */
		  "",
		  0 },

		/* terminate */
		{ NULL, NULL, 0 }
	};

	int verbose = 0;

	if (argc > 1 && strcmp(argv[1], "-v") == 0) verbose = 1;

	perform_test(test_newstyle, newstyle_cases, verbose);
	perform_test(escape_spaces, escape_spaces_cases, verbose);
	printf("Test passed.\n");
	return 0;
}
