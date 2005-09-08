/*
#  File:     job_status.c
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#
#  Description:
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
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

#include "classad_c_helper.h"
#include "blahpd.h"
#include "mtsafe_popen.h"

extern char *blah_script_location;

int
get_status(const char *jobDesc, classad_context *cad, char *error_str, int get_workernode)
{
	FILE *cmd_out;
	char buffer[1024];
	char *command;
	char *cad_str = NULL;
	int retcode = 0;
	char *server_lrms;
	char *jobId;

        if (strlen(jobDesc) < 4)
        {
		strncpy(error_str, "Malformed jobId", ERROR_MAX_LEN);
                return(255);
        }

        if((server_lrms = strdup(jobDesc)) == NULL)
	{
                fprintf(stderr, "Out of memory\n");
                exit(MALLOC_ERROR);
        }

        server_lrms[3] = '\0';
        jobId = server_lrms + 4;

        command = make_message("%s/%s_status.sh %s %s", blah_script_location, server_lrms, (get_workernode ? "-w" : ""), jobId);

	/* fprintf(stderr, "DEBUG: status cmd = %s\n", command); */
	if ((cmd_out=mtsafe_popen(command, "r")) == NULL)
	{
		fprintf(stderr, "Unable to execute '%s': ", command);
		perror("");
		strncpy(error_str, "Unable to open pipe for status command", ERROR_MAX_LEN);
		free(server_lrms);
		return(255);
	}

	if((cad_str = malloc(1)) == NULL)
	{
		fprintf(stderr, "Out of memory\n");
		exit(MALLOC_ERROR);
	}
	cad_str[0] = '\0';

	while (fgets(buffer, sizeof(buffer), cmd_out))
	{
		/* This line is for backward compatibility */
		if (strcmp(buffer, "*** END ***\n") == 0) break;
		if (buffer[strlen(buffer) - 1] == '\n') buffer[strlen(buffer) - 1] = ' ';
		cad_str = (char *) realloc (cad_str, strlen(cad_str) + strlen(buffer) + 1);
		strcat(cad_str, buffer);
	}

	retcode = mtsafe_pclose(cmd_out);
	if (retcode)
	{
		*cad = NULL;
		strncpy(error_str, cad_str, ERROR_MAX_LEN);
	}
	else
	{
		/* fprintf(stderr, "DEBUG: classad = %s\n", cad_str); */
		*cad = classad_parse(cad_str);
		if (*cad == NULL)
		{
			strncpy(error_str, "Error parsing classad", ERROR_MAX_LEN);
                        free(cad_str);
                        free(command);
			free(server_lrms);
			return(255);
		}
	}

	free(server_lrms);
	free(cad_str);
	free(command);

	return WEXITSTATUS(retcode);
}
