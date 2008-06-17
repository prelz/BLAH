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
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

#include "classad_c_helper.h"
#include "blahpd.h"
#include "mtsafe_popen.h"
#include "job_registry.h"
#define MAX_TEMP_ARRAY_SIZE              1000
#define CAD_LEN                          1024

extern char *blah_script_location;
extern int  glexec_mode;
extern char *gloc;
extern job_registry_handle *blah_jr_handle;
extern pthread_mutex_t blah_jr_lock;

#define TSF_DEBUG

int unlink_proxy_symlink(const char *jobDesc, classad_context *cad, char **environment)
{
	job_registry_split_id *spid;
	int job_status;
	char *proxy_link, *command;
	int retcod=-2;
	char *cmd_out;

	if((spid = job_registry_split_blah_id(jobDesc)) != NULL)
	{
		if (classad_get_int_attribute(cad, "JobStatus", &job_status) == C_CLASSAD_NO_ERROR)
		{
			if (job_status == (int)REMOVED || job_status == (int)COMPLETED )
			{
				if (*environment) /* GLEXEC Mode ? */
				{
					command = make_message("%s /bin/rm .blah_jobproxy_dir/%s.proxy", gloc, spid->proxy_id);
				}
				else
				{
					command = make_message("/bin/rm %s/.blah_jobproxy_dir/%s.proxy", getenv("HOME"), spid->proxy_id);
				}
				if (command != NULL)
				{	
					retcod = exe_getout(command, environment, &cmd_out);
					if (cmd_out) free (cmd_out);
					free(command);
				}
				if (retcod != 0) /* Try the '.norenew' file in case of failure. */
				{
					if (*environment) /* GLEXEC Mode ? */
					{
						command = make_message("%s /bin/rm .blah_jobproxy_dir/%s.proxy.norenew", gloc, spid->proxy_id);
					}
					else
					{
						command = make_message("/bin/rm %s/.blah_jobproxy_dir/%s.proxy.norenew", getenv("HOME"), spid->proxy_id);
					}
					if (command != NULL)
					{	
						retcod = exe_getout(command, environment, &cmd_out);
						if (cmd_out) free (cmd_out);
						free(command);
					}
				}
			}
		}
		job_registry_free_split_id(spid);
	}
	return(retcod);
}

int get_status(const char *jobDesc, classad_context *cad, char **environment, char error_str[][ERROR_MAX_LEN], int get_workernode, int *job_nr)
{
	char *cmd_out;
	char *command;
	char cad_str[100][CAD_LEN];
	int  retcode = 0;
	job_registry_split_id *spid;
	int  i, lc;
	classad_context tmpcad;
	char *cadstr;
	int res_length;
	char *begin_res;
	char *end_res;
	job_registry_entry *ren;

	/* Look up job registry first, if configured. */
	if (blah_jr_handle != NULL)
	{
		/* File locking will not protect threads in the same */
		/* process. */
	 	pthread_mutex_lock(&blah_jr_lock);
		if ((ren=job_registry_get(blah_jr_handle, jobDesc)) != NULL)
		{
			if (!get_workernode) ren->wn_addr[0]='\000';
			cadstr = job_registry_entry_as_classad(blah_jr_handle, ren);                       
			if (cadstr != NULL)
			{
				tmpcad=classad_parse(cadstr);
				free(cadstr);
				if (tmpcad != NULL)
				{
					/* Need to undo the proxy symlink as the status scripts do. */
					/* FIXME: This can be removed when the proxy file is moved */
					/* FIXME: into the registry. */
					unlink_proxy_symlink(jobDesc, tmpcad, environment);					
					*job_nr = 1;
					strcpy(error_str[0],"No Error");
					cad[0] = tmpcad;
	 				pthread_mutex_unlock(&blah_jr_lock);
					return 0;
				}
			}
		}
	 	pthread_mutex_unlock(&blah_jr_lock);
        }

	/* If we reach here, any of the above telescope went wrong and, for */
	/* the time being, we fall back to the old script approach */

	if((spid = job_registry_split_blah_id(jobDesc)) == NULL)
	{
		/* PUSH A FAILURE */
		strcpy(*error_str, "Malformed jobId or out of memory");
		return(255);
	}

		command = make_message("%s %s/%s_status.sh %s %s", *environment ? gloc : "", 
	                         blah_script_location, spid->lrms, (get_workernode ? "-w" : ""), jobDesc);

	if ((retcode = exe_getout(command, environment, &cmd_out)) != 0)
        {
		sprintf(*error_str, "status command failed (exit code %d)", retcode);
		job_registry_free_split_id(spid);
		free(command);
		if (cmd_out) free (cmd_out);
                return(255);
        }
	free(command);
	job_registry_free_split_id(spid);

	lc = 0;
	res_length = strlen(cmd_out);
	for (begin_res = cmd_out; end_res = memchr(cmd_out, '\n', res_length); begin_res = end_res + 1)
	{
		*end_res = 0;
		if (begin_res[0] != '1')
			strncpy(cad_str[lc], begin_res + 1, CAD_LEN - 1);
		else
			cad_str[lc][0] = '\0';
		lc++;
	}
	free(cmd_out);

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

