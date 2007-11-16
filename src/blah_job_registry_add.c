/*
 *  File :     blah_job_registry_add.c
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  16-Nov-2007 Original release
 *
 *  Description:
 *   Executable to add (append or update) an entry to the job registry.
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
  job_registry_entry en;
  job_status_t status=IDLE;
  int exitcode = -1; 
  char *wn_addr = "";
  time_t udate=0;
  char *blah_id, *batch_id;
  int ret;

  if ((registry_file = getenv("BLAH_REGISTRY_FILE")) == NULL)
   {
    my_home = getenv("HOME");
    if (my_home == NULL) my_home = ".";
    registry_file = (char *)malloc(strlen(default_registry_file)+strlen(my_home)+2);
    if (registry_file != NULL) 
      sprintf(registry_file,"%s/%s",my_home,default_registry_file);
    else return 1;
   }
 
  if (argc < 3)
   {
    fprintf(stderr,"Usage: %s <BLAH id> <batch id> [job status] [exit code] [udate] [worker node]\n",argv[0]);
    return 1;
   }

  blah_id  = argv[1]; 
  batch_id = argv[2]; 

  if (argc > 3) status = atoi(argv[3]);
  if (argc > 4) exitcode = atoi(argv[4]);
  if (argc > 5) udate = atol(argv[5]);
  if (argc > 6) wn_addr = argv[6];
   
  job_registry_handle *rha;

  rha=job_registry_init(registry_file, BY_BLAH_ID);

  if (rha == NULL)
   {
    fprintf(stderr,"%s: error initialising job registry: ",argv[0]);
    perror("");
    return 2;
   }

  JOB_REGISTRY_ASSIGN_ENTRY(en.blah_id,blah_id); 
  JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_id); 
  en.status = status;
  en.exitcode = exitcode;
  JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,wn_addr); 
  en.udate = udate;
    
  if ((ret=job_registry_append(rha, &en)) < 0)
   {
    fprintf(stderr,"%s: job_registry_append returns %d: ",argv[0],ret);
    perror("");
   } 

  job_registry_destroy(rha);
  return 0;
}
