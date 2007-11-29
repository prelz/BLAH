/*
 *  File :     test_job_registry_update.c
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  15-Nov-2007 Original release
 *
 *  Description:
 *   Update test for job registries created by test_job_registry_create.
 *   This is meant to run at the same time as test_job_registry_access.
 *
 *  Copyright (c) 2007 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include "job_registry.h"

int
main(int argc, char *argv[])
{
  char *test_registry_file = JOB_REGISTRY_TEST_FILE;
  job_registry_entry en;
  int ret;
  int pick;
  struct timeval tm_start, tm_end;
  float elapsed_secs;
  int n_update_tests;
  int i;

  if (argc > 1) test_registry_file = argv[1];

  srand(time(0));

  job_registry_handle *rha;

  gettimeofday(&tm_start, NULL);
  rha=job_registry_init(test_registry_file, BY_BLAH_ID);
  gettimeofday(&tm_end, NULL);

  if (rha == NULL)
   {
    fprintf(stderr,"%s: error initialising job registry: ",argv[0]);
    perror("");
    return 1;
   }

  if (rha->n_entries <= 0)
   {
    fprintf(stderr,"%s: job registry %s has %d entries. Little to do.\n",
            argv[0], test_registry_file, rha->n_entries);
    job_registry_destroy(rha);
    return 1;
   }

  if (argc > 2) n_update_tests = atoi(argv[2]);
  else          n_update_tests = rha->n_entries*2;

  elapsed_secs = (tm_end.tv_sec - tm_start.tv_sec) +
                 (float)(tm_end.tv_usec - tm_start.tv_usec)/1000000;
  printf("%s: Successfully indexed %d entries in %g seconds (%g entries/s)."
         "\nNow performing %d updates.\n",
         argv[0],rha->n_entries, elapsed_secs, rha->n_entries/elapsed_secs,
         n_update_tests);

  gettimeofday(&tm_start, NULL);
  for (i=0; i<n_update_tests; i++)
   {
    pick = rand()%(rha->n_entries);
    en.udate = time(0);
    en.status = rand()%6;
    en.exitcode = rand()%128;
    memcpy(en.blah_id,rha->entries[pick].id,sizeof(en.blah_id));
    en.batch_id[0]='\000';

    ret = job_registry_update(rha, &en);
    if (ret != JOB_REGISTRY_SUCCESS)
     {
      fprintf(stderr,"%s: job_registry_update of ID==%s returns %d: ",
              argv[0], rha->entries[pick].id, ret);
      perror("");
      job_registry_destroy(rha);
      return 1;
     }
   }
  gettimeofday(&tm_end, NULL);
  elapsed_secs = (tm_end.tv_sec - tm_start.tv_sec) +
                 (float)(tm_end.tv_usec - tm_start.tv_usec)/1000000;
  printf("%s: updated %d entries in %g seconds (%g entries/s)\n", argv[0],
         n_update_tests, elapsed_secs, n_update_tests/elapsed_secs);

  job_registry_destroy(rha);
  return 0;
}
