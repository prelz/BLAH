/*
 *  File :     proxy_hashcontainer.h
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  12-Dec-2006 Original release
 *
 *  Description:
 *    Prototypes of functions defined in proxy_hashcontainer.c
 *
 *  Copyright (c) 2006 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#ifndef __PROXY_HASHCONTAINER_H__
#define __PROXY_HASHCONTAINER_H__

typedef struct proxy_hashcontainer_entry_s
 {
   char *id;
   char *proxy_file_name;
   struct proxy_hashcontainer_entry_s *next;
 } proxy_hashcontainer_entry;

#define PROXY_HASHCONTAINER_SUCCESS    0
#define PROXY_HASHCONTAINER_NOT_FOUND -1 

void proxy_hashcontainer_init();
unsigned int proxy_hashcontainer_hashfunction(char *id);
proxy_hashcontainer_entry *proxy_hashcontainer_lookup(char *id);
proxy_hashcontainer_entry *proxy_hashcontainer_new(char *id, char *proxy_file_name);
void proxy_hashcontainer_free(proxy_hashcontainer_entry *entry);
void proxy_hashcontainer_cleanup(void);
proxy_hashcontainer_entry *proxy_hashcontainer_append(char *id, char *proxy_file_name);
int proxy_hashcontainer_unlink(char *id);
proxy_hashcontainer_entry *proxy_hashcontainer_add(char *id, char *proxy_file_name);

#endif /*defined __PROXY_HASHCONTAINER_H__*/
