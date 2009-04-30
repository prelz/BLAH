/*
#  File:     env_helper.h
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#    10 Mar 2009 - Original release
#
#  Description:
#    Helper functions to manage an execution environment.
#
#
#  Copyright (c) 2009 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/

#ifndef ENV_HELPER_H_INCLUDED
#define ENV_HELPER_H_INCLUDED

typedef char ** env_t;

int push_env(env_t *my_env, const char *new_env);
int copy_env(env_t *rc_dest, const env_t env_src);
int append_env(env_t *rc_dest, const env_t env_src);
void free_env(env_t *my_env);

#endif /*ENV_HELPER_H_INCLUDED*/ 
