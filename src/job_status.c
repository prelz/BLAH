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
#include "mapped_exec.h"
#include "job_registry.h"
#include "blah_utils.h"

#define MAX_TEMP_ARRAY_SIZE              1000
#define CAD_LEN                          1024

extern char *blah_script_location;
extern job_registry_handle *blah_jr_handle;
extern pthread_mutex_t blah_jr_lock;

#define TSF_DEBUG

int
unlink_proxy_symlink(const char *jobDesc, classad_context *cad, char **deleg_parameters)
{
	job_registry_split_id *spid;
	int job_status;
	char *proxy_link;
	int retcod = -2;
	exec_cmd_t exec_command = EXEC_CMD_DEFAULT;

	if ((spid = job_registry_split_blah_id(jobDesc)) != NULL)
	{
		if (classad_get_int_attribute(cad, "JobStatus", &job_status) == C_CLASSAD_NO_ERROR)
		{
			if (job_status == (int)REMOVED || job_status == (int)COMPLETED )
			{
				if (*deleg_parameters) /* GLEXEC Mode ? */
				{
					exec_command.delegation_type = atoi(deleg_parameters[MEXEC_PARAM_DELEGTYPE]);
					exec_command.delegation_cred = deleg_parameters[MEXEC_PARAM_DELEGCRED];
					/* Proxy link location is relative to the HOME of the *mapped* user in this case. */
					exec_command.command = make_message("/bin/rm .blah_jobproxy_dir/%s.proxy",
			 							spid->proxy_id);
				} else {
					exec_command.command = make_message("/bin/rm %s/.blah_jobproxy_dir/%s.proxy",
										getenv("HOME"), spid->proxy_id);
				}

				if (exec_command.command == NULL)
				{
					fprintf(stderr, "blahpd: out of memory");
					exit(1);
				}

				retcod = execute_cmd(&exec_command);

				if (exec_command.exit_code != 0) /* Try the '.norenew' file in case of failure. */
				{
					recycle_cmd(&exec_command);
					exec_command.append_to_command = ".norenew";
					execute_cmd(&exec_command);
				}
				free(exec_command.command);
				cleanup_cmd(&exec_command);
			}
		}
		job_registry_free_split_id(spid);
	}
	return(retcod);
}

int
get_status(const char *jobDesc, classad_context *cad, char **deleg_parameters, char error_str[][ERROR_MAX_LEN], int get_workernode, int *job_nr)
{
	char cad_str[100][CAD_LEN];
	int  retcode = 0, exitcode = 0;
	job_registry_split_id *spid;
	int  i, lc = 0;
	classad_context tmpcad;
	char *cadstr;
	int res_length;
	char *begin_res;
	char *end_res;
	job_registry_entry *ren;
	exec_cmd_t exec_command = EXEC_CMD_DEFAULT;

	/* Look up job registry first, if configured. */
	if (blah_jr_handle != NULL)
	{
		/* File locking will not protect threads in the same */
		/* process. */
	 	pthread_mutex_lock(&blah_jr_lock);
		if ((ren = job_registry_get(blah_jr_handle, jobDesc)) != NULL)
		{
			if (!get_workernode) ren->wn_addr[0]='\000';
			cadstr = job_registry_entry_as_classad(blah_jr_handle, ren);                       
			if (cadstr != NULL)
			{
				tmpcad = classad_parse(cadstr);
				free(cadstr);
				if (tmpcad != NULL)
				{
					/* Need to undo the proxy symlink as the status scripts do. */
					/* FIXME: This can be removed when the proxy file is moved */
					/* FIXME: into the registry. */
					unlink_proxy_symlink(jobDesc, tmpcad, deleg_parameters);					
					*job_nr = 1;
					strncpy(error_str[0], "No Error", ERROR_MAX_LEN);
					cad[0] = tmpcad;
	 				pthread_mutex_unlock(&blah_jr_lock);
					free(ren);
					return 0;
				}
			}
			free(ren);
		}
	 	pthread_mutex_unlock(&blah_jr_lock);
	}

	/* If we reach here, any of the above telescope went wrong and, for */
	/* the time being, we fall back to the old script approach */

	if((spid = job_registry_split_blah_id(jobDesc)) == NULL)
	{
		/* PUSH A FAILURE */
		strncpy(*error_str, "Malformed jobId or out of memory", ERROR_MAX_LEN);
		return(255);
	}

	exec_command.command = make_message("%s/%s_status.sh %s %s", blah_script_location,
	                                    spid->lrms, (get_workernode ? "-w" : ""), jobDesc);
	if (exec_command.command == NULL)
	{
		fprintf(stderr, "blahpd: out of memory");
		exit(1);
	}

	if (deleg_parameters) /* glexec mode */
	{
		exec_command.delegation_type = atoi(deleg_parameters[MEXEC_PARAM_DELEGTYPE]);
		exec_command.delegation_cred = deleg_parameters[MEXEC_PARAM_DELEGCRED];
	}

	retcode = execute_cmd(&exec_command);

	if (retcode != 0) 
	{
		snprintf(*error_str, ERROR_MAX_LEN, "error invoking status command: %s", strerror(errno)); /*FIXME: strerror not thrad safe*/
		exitcode = 255;
		goto free_cmd;
	}

	if (exec_command.exit_code != 0)
	{
		snprintf(*error_str, ERROR_MAX_LEN, "status command failed: %s", exec_command.error);
		exitcode = 255;
		goto cleanup_cmd;
	}

	res_length = strlen(exec_command.output);
	for (begin_res = exec_command.output; end_res = memchr(exec_command.output, '\n', res_length); begin_res = end_res + 1)
	{
		*end_res = 0;
		if (begin_res[0] != '1')
			strncpy(cad_str[lc], begin_res + 1, CAD_LEN - 1);
		else
			cad_str[lc][0] = '\0';
		lc++;
	}

	for(i = 0; i < lc; i++)
	{
		if (cad_str[i][0] == '\0')
		{
			/* Error parsing classad or job not found */
			cad[i]  = NULL; 
			strncpy((error_str[i]),"Error parsing classad or job not found", ERROR_MAX_LEN);
		}
		else
		{
			tmpcad = classad_parse(cad_str[i]);                       
			if (tmpcad == NULL)
			{
				strncpy((error_str[i]), "Error allocating memory", ERROR_MAX_LEN);
				return(1);
			}
			cad[i] = tmpcad;
			strncpy(error_str[i], "No Error", ERROR_MAX_LEN);
		}
	}
	
cleanup_cmd:
	cleanup_cmd(&exec_command);
free_cmd:
	free(exec_command.command);
free_split_id:
	job_registry_free_split_id(spid);

	*job_nr = lc;
	return(exitcode);
}

