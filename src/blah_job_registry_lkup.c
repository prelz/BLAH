/*
 *  File :     blah_job_registry_lkup.c
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  16-Nov-2007 Original release
 *
 *  Description:
 *   Executable to look up for an entry in the BLAH job registry.
 *
 *  Copyright (c) 2007 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include "job_registry.h"

int
main(int argc, char *argv[])
{
  char *registry_file;
  const char *default_registry_file = "blah_job_registry.bjr";
  char *my_home;
  job_registry_index_mode mode=BY_BLAH_ID;
  char *id;
  job_registry_entry *ren;

  if ((registry_file = getenv("BLAH_REGISTRY_FILE")) == NULL)
   {
    my_home = getenv("HOME");
    if (my_home == NULL) my_home = ".";
    registry_file = (char *)malloc(strlen(default_registry_file)+strlen(my_home)+2);
    if (registry_file != NULL) 
      sprintf(registry_file,"%s/%s",my_home,default_registry_file);
    else return 1;
   }
 
  if (argc < 2)
   {
    fprintf(stderr,"Usage: %s [-b to look up batch IDs] <id>\n",argv[0]);
    return 1;
   }

  id = argv[1];

  if (argc > 2)
   {
    if (strcmp(argv[1],"-b") == 0) 
     {
      mode = BY_BATCH_ID;
      id = argv[2];
     }
   }

  job_registry_handle *rha;

  rha=job_registry_init(registry_file, mode);

  if (rha == NULL)
   {
    fprintf(stderr,"%s: error initialising job registry: ",argv[0]);
    perror("");
    return 2;
   }

  if ((ren=job_registry_get(rha, id)) == NULL)
   {
    fprintf(stderr,"%s: Entry <%s> not found: ",argv[0],id);
    perror("");
    job_registry_destroy(rha);
    return 1;
   } 

  printf("recnum   == %d\n",ren->recnum);
  printf("blah_id  == <%s>\n",ren->blah_id);
  printf("batch_id == <%s>\n",ren->batch_id);
  printf("cdate    == %d - %s",ren->cdate,ctime(&ren->cdate));
  printf("mdate    == %d - %s",ren->mdate,ctime(&ren->mdate));
  printf("udate    == %d - %s",ren->udate,ctime(&ren->udate));
  printf("status   == %d\n",ren->status);
  printf("exitcode == %d\n",ren->exitcode);
  printf("wn_addr  == <%s>\n",ren->wn_addr);

  free(ren);
  job_registry_destroy(rha);
  return 0;
}
