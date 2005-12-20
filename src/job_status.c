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
extern int  glexec_mode;
extern char *gloc;

int get_status(const char *jobDesc, classad_context *cad, char error_str[][ERROR_MAX_LEN], int get_workernode, int *job_nr)
{
        FILE *cmd_out;
        char buffer[1024];
        char *command;
        char cad_str[100][1024];
        int  retcode = 0;
        char *server_lrms;
        char *jobId;
        int   i, lc;
        classad_context tmpcad;
        char **tmperrstr = NULL;

	if (strlen(jobDesc) < 4)
        {
		strcpy(*error_str,"Malformed jobId");
		return(255);
        }

        if((server_lrms = strdup(jobDesc)) == NULL)
        {
                fprintf(stderr, "Out of memory\n");
                exit(MALLOC_ERROR);
        }

        server_lrms[3] = '\0';
	if(!glexec_mode)
	{
		command = make_message("%s/%s_status.sh %s %s", blah_script_location, server_lrms, (get_workernode ? "-w" : ""), jobDesc);
        }else
		command = make_message("%s %s/%s_status.sh %s %s", gloc, blah_script_location, server_lrms, (get_workernode ? "-w" : ""), jobDesc);
	if ((cmd_out=mtsafe_popen(command, "r")) == NULL)
        {
                fprintf(stderr, "Unable to execute '%s': ", command);
                perror("");
		strcpy(*error_str,"Unable to open pipe for status command");
		free(server_lrms);
                return(255);
        }
	free(command);
	free(server_lrms);
	command = server_lrms = NULL;
        lc = 0;
        while (fgets(buffer, sizeof(buffer), cmd_out))
        {
                if (buffer[strlen(buffer) - 1] == '\n') buffer[strlen(buffer) - 1] = ' ';
                //retcode  from scripts != 0
		lc++;
		if(buffer[0] != '1')
		{
			strncpy(cad_str[lc - 1],buffer + 1,strlen(buffer) - 1);
		}else
			cad_str[lc - 1][0] = '\0';
		memset(buffer,0,strlen(buffer));
        }
        retcode = mtsafe_pclose(cmd_out);
	
	for(i = 0; i < lc; i++)
        {
			if (cad_str[i][0] == '\0')
                        {
				//Error parsing classad or job not found
				cad[i]  = NULL; 
				strcpy((error_str[i]),"Error parsing classad or job not found");	
			}else
                        {
				tmpcad = classad_parse(cad_str[i]);                       
				if (tmpcad == NULL)
                                {
					strcpy((error_str[i]),"Error allocating memory");
					return(1);
                                }
                        	cad[i] = tmpcad;
				strcpy(error_str[i],"No Error");
			}
          }
	
	*job_nr  = lc;
        //return WEXITSTATUS(retcode);
	return 0;
}

