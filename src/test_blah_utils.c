#include <stdio.h>
#include <wordexp.h>
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
