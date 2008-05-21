/*
 *  File :     blah_job_registry_add.c
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  16-Nov-2007 Original release
 *  27-Feb-2008 Added user_prefix.
 *   3-Mar-2008 Added non-privileged updates to fit CREAM's file and process
 *              ownership model.
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
#include <unistd.h> /* geteuid() */
#include <time.h>
#include <errno.h>
#include "job_registry.h"
#include "config.h"

int
main(int argc, char *argv[])
{
  char *registry_file = NULL, *registry_file_env = NULL;
  int need_to_free_registry_file = FALSE;
  const char *default_registry_file = "blah_job_registry.bjr";
  char *my_home;
  job_registry_entry en;
  job_status_t status=IDLE;
  int exitcode = -1; 
  char *exitreason = "";
  char *user_prefix = "";
  char *wn_addr = "";
  time_t udate=0;
  char *blah_id, *batch_id;
  int ret;
  config_handle *cha;
  config_entry *rge;
  job_registry_handle *rha, *rhano;

  if (argc < 3)
   {
    fprintf(stderr,"Usage: %s <BLAH id> <batch id> [job status] [udate] [user prefix] [worker node] [exit code] [exit reason]\n",argv[0]);
    return 1;
   }

  blah_id  = argv[1]; 
  batch_id = argv[2]; 

  if (argc > 3) status = atoi(argv[3]);
  if (argc > 4) udate = atol(argv[4]);
  if (argc > 5) user_prefix = argv[5];
  if (argc > 6) wn_addr = argv[6];
  if (argc > 7) exitcode = atoi(argv[7]);
  if (argc > 8) exitreason = argv[8];
   
  cha = config_read(NULL); /* Read config from default locations. */
  if (cha != NULL)
   {
    rge = config_get("job_registry", cha);
    if (rge != NULL) registry_file = rge->value;
   }

  /* Env variable takes precedence */
  registry_file_env = getenv("BLAH_JOB_REGISTRY_FILE");
  if (registry_file_env != NULL) registry_file = registry_file_env;

  if (registry_file == NULL)
   {
    my_home = getenv("HOME");
    if (my_home == NULL) my_home = ".";
    registry_file = (char *)malloc(strlen(default_registry_file)+strlen(my_home)+2);
    if (registry_file != NULL)
     {
      sprintf(registry_file,"%s/%s",my_home,default_registry_file);
      need_to_free_registry_file = TRUE;
     }
    else 
     {
      if (cha != NULL) config_free(cha);
      return 1;
     }
   }

  JOB_REGISTRY_ASSIGN_ENTRY(en.blah_id,blah_id); 
  JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_id); 
  en.status = status;
  en.exitcode = exitcode;
  JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,wn_addr); 
  en.udate = udate;
  JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,exitreason); 
  en.submitter = geteuid();
  JOB_REGISTRY_ASSIGN_ENTRY(en.user_prefix,user_prefix); 
    
  rha=job_registry_init(registry_file, BY_BLAH_ID);

  if (rha == NULL)
   {
    if (errno == EACCES)
     {
      /* Try nonpriv update. It may work. */
      rhano = job_registry_init(registry_file, NAMES_ONLY);
      if (cha != NULL) config_free(cha);
      if (need_to_free_registry_file) free(registry_file);
      if (rhano != NULL)
       {
        ret=job_registry_append_nonpriv(rhano, &en);
        job_registry_destroy(rhano);
        if (ret < 0)
         {
          fprintf(stderr,"%s: job_registry_append_nonpriv returns %d: ",argv[0],ret);
          perror("");
          return 4;
         } 
        else return 0;
       }
     }
    else
     {
      fprintf(stderr,"%s: error initialising job registry: ",argv[0]);
      perror("");
     }
    return 2;
   }

  /* Filename is stored in job registry handle. - Don't need these anymore */
  if (cha != NULL) config_free(cha);
  if (need_to_free_registry_file) free(registry_file);

  if ((ret=job_registry_append(rha, &en)) < 0)
   {
    if (errno == EACCES)
     {
      /* Try nonpriv update. It may work. */
      ret=job_registry_append_nonpriv(rha, &en);
      job_registry_destroy(rha);
      if (ret < 0)
       {
        fprintf(stderr,"%s: job_registry_append_nonpriv returns %d: ",argv[0],ret);
        perror("");
        return 5;
       } 
      else return 0;
     }

    fprintf(stderr,"%s: job_registry_append returns %d: ",argv[0],ret);
    perror("");
    job_registry_destroy(rha);
    return 3;
   } 

  job_registry_destroy(rha);
  return 0;
}
