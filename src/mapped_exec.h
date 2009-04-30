/*
#  File:     deleg_exec.h
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#    10 Mar 2009 - Original release
#
#  Description:
#    Executes a command, enabling optional "sudo-like" mechanism (like glexec or sudo itself).
#
#
#  Copyright (c) 2009 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/

#ifndef MAPPED_EXEC_H_INCLUDED
#define MAPPED_EXEC_H_INCLUDED

#include "env_helper.h"

#define MEXEC_GET_NO_OUTPUT 0
#define MEXEC_GET_STDOUT    1
#define MEXEC_GET_STDERR    2
#define MEXEC_GET_STDBOTH   3

typedef enum delegation_type_e
{
	MEXEC_NO_MAPPING = 0,
	MEXEC_GLEXEC,
	MEXEC_SUDO
} mapping_t;

typedef enum special_commands_e
{
	MEXEC_NORMAL_COMMAND = 0,
	MEXEC_KILL_COMMAND,
	MEXEC_PROXY_COMMAND
} cmdtype_t;

typedef enum delegation_param_idx_e
{
	MEXEC_PARAM_DELEGTYPE = 0,
	MEXEC_PARAM_DELEGCRED,
	MEXEC_PARAM_SRCPROXY,
	MEXEC_PARAM_COUNT
} delegation_param_idx_t;

typedef struct exec_cmd_s
{
	char *    command;            /* the command to execute */
	char *    append_to_command;  /* optional string appended to the command */ 
	mapping_t delegation_type;    /* one of the above delegation_type constants */
	char *    delegation_cred;    /* the semantic depends on delegation type */
	char *    source_proxy;       /* proxy to be copied for the mapped user */
	char *    dest_proxy;         /* destination file for the mapped user proxy */
	int       copy_original_env;  /* boolean flag (if true, current environment is copied to the child) */
	env_t     environment;        /* additional environment for the command */
	cmdtype_t special_cmd;        /* used to avoid infinite recursion of some special commands */
	int       flags;              /* select which stream to retrieve (future enhancement, not yet implemented) */
	char *    output;             /* dynamically allocated string for the command's stdout */
	char *    error;              /* dynamically allocated string for the command's stderr */
	int       exit_code;          /* the command's exit code */ 
} exec_cmd_t;

/* Sensible defaults for a normal, non mapped command */
#define EXEC_CMD_DEFAULT {NULL, NULL, MEXEC_NO_MAPPING, NULL, NULL, NULL, 1, NULL, MEXEC_NORMAL_COMMAND, MEXEC_GET_STDBOTH, NULL, NULL, 0}

int execute_cmd(exec_cmd_t *cmd); /* execute the command. Returns 0 on success or -1 and the proper value in errno */
                                  /* on failure. N.B. returned value is not the command's exit code (use           */
                                  /* cmd->exit_code for that).                                                     */
void cleanup_cmd(exec_cmd_t *cmd); /* free up all the dynamically allocated memory for the structure                */
void recycle_cmd(exec_cmd_t *cmd); /* free only output and error, to reuse the cmd structure for a new execution    */

#endif /*MAPPED_EXEC_H_INCLUDED*/
