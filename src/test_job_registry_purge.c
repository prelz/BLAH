/*
 *  File :     test_job_registry_scan.c
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  15-Nov-2007 Original release
 *
 *  Description:
 *   Collect contents statistics from a test job registry.
 *
 *  Copyright (c) 2007 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "job_registry.h"

int
main(int argc, char *argv[])
{
  char *test_registry_file = JOB_REGISTRY_TEST_FILE;
  job_registry_entry *en;
  time_t old_date;
  time_t first_cdate=0;
  time_t last_cdate;
  int count[10];
  FILE *fd;
  int ret;
  int i;

  if (argc > 1) test_registry_file = argv[1];

  if (argc > 2) 
   {
    old_date = atol(argv[2]);
    job_registry_purge(test_registry_file, old_date);
   }

  job_registry_handle *rha;

  rha=job_registry_init(test_registry_file, NO_INDEX);

  if (rha == NULL)
   {
    fprintf(stderr,"%s: error initialising job registry %s: ",argv[0],test_registry_file);
    perror("");
    return 1;
   }

  printf("%s: job registry %s contains %d entries.\n",argv[0],
         test_registry_file, rha->lastrec - rha->firstrec);

  fd = job_registry_open(rha, "r");
  if (fd == NULL)
   {
    fprintf(stderr,"%s: Error opening registry %s: ",argv[0],test_registry_file);
    perror("");
    return 1;
   }
  if (job_registry_rdlock(rha, fd) < 0)
   {
    fprintf(stderr,"%s: Error read locking registry %s: ",argv[0],test_registry_file);
    perror("");
    return 1;
   }
 
  for (i=0;i<10;i++) count[i] = 0;

  while ((en = job_registry_get_next(rha, fd)) != NULL)
   {
    if (first_cdate == 0) first_cdate = en->cdate;
    last_cdate = en->cdate;
    count[en->status]++;    
    free(en); 
   }

  printf("First cdate == %d\n", first_cdate);
  printf("Last  cdate == %d\n", last_cdate);
  for (i=0;i<10;i++) printf("count[%2d] == %d\n",i,count[i]);

  job_registry_destroy(rha);
  return 0;
}
