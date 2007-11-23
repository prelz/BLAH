/*
 *  File :     config.h
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  23-Nov-2007 Original release
 *
 *  Description:
 *    Prototypes of functions defined in config.c
 *
 *  Copyright (c) 2007 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#ifndef __CONFIG_H__
#define __CONFIG_H__

#include <stdio.h>

#include "blahpd.h"

typedef struct config_entry_s
 {
   char *key;
   char *value;
   struct config_entry_s *next;
 } config_entry;

typedef struct config_handle_s
 {
   char *install_path;
   char *bin_path;
   char *config_path;
   config_entry *list;
 } config_handle;

config_handle *config_read(const char *path);
config_entry *config_get(const char *key, config_handle *handle);
int config_test_boolean(const config_entry *entry);
void config_free(config_handle *handle);

#define CONFIG_FILE_BASE "blah_config.h"

#define CONFIG_SKIP_WHITESPACE_FWD(c) while ((*(c) == ' ')  || (*(c) == '\t') || \
                                  (*(c) == '\n') || (*(c) == '\r') ) (c)++;
#define CONFIG_SKIP_WHITESPACE_BCK(c) while ((*(c) == ' ')  || (*(c) == '\t') || \
                                  (*(c) == '\n') || (*(c) == '\r') ) (c)--;

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#endif /*defined __CONFIG_H__*/
