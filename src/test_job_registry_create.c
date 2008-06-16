/*
 *  File :     test_job_registry_create.c
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  14-Nov-2007 Original release
 *  27-Feb-2008 Added user_prefix.
 *
 *  Description:
 *    Job registry creation test for code in job_registry.{c,h}
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

int
main(int argc, char *argv[])
{
  char *test_registry_file = JOB_REGISTRY_TEST_FILE;
  char *test_blahid_format="conlrms/%c%c%c_blahid_%05d/stuff";
  char *test_batchid_format="%c%c%c_batchid_%05d";
  char *test_proxy;
  job_registry_entry en;
  struct timeval tm_start, tm_end;
  int n_entries = 10000;
  float elapsed_secs;
  int ret;
  int i;

  if (argc > 1) test_registry_file = argv[1];
  if (argc > 2) n_entries = atoi(argv[2]);

  srand(time(0));

  job_registry_handle *rha;

  rha=job_registry_init(test_registry_file, NO_INDEX);

  if (rha == NULL)
   {
    fprintf(stderr,"%s: error initialising job registry: ",argv[0]);
    perror("");
    return 1;
   }

  test_proxy = getenv("X509_USER_PROXY");
  if (test_proxy == NULL) test_proxy = "/tmp/jrtest.proxy";

  gettimeofday(&tm_start, NULL);
  /* Create and append n_entries entries */
  for(i=0;i<n_entries;i++)
   {
    /* Make sure we don't include a slash (ASCII 47) in the BLAH ID */
    sprintf(en.blah_id, test_blahid_format, rand()%78+48, rand()%78+48,
                           rand()%78+48, i);
    sprintf(en.batch_id, test_batchid_format, rand()%94+33, rand()%94+33,
                           rand()%94+33, i);
    en.status = IDLE;
    en.exitcode = -1;
    en.wn_addr[0]='\000';
    en.exitreason[0] = '\000';
    en.submitter = geteuid();
    if ((rand()%100) > 70) 
     {
      JOB_REGISTRY_ASSIGN_ENTRY(en.user_prefix,"testp_");
     }
    else                   en.user_prefix[0] = '\000';
    
    if ((rand()%100) > 70)
     {
      job_registry_set_proxy(rha, &en, test_proxy);
      en.renew_proxy = 1;
     }
    else                   en.proxy_link[0]='\000';

    if ((ret=job_registry_append(rha, &en)) < 0)
     {
      fprintf(stderr,"%s: Append of record #%05d returns %d: ",argv[0],i,ret);
      perror("");
     } 
   }
  gettimeofday(&tm_end, NULL);
  elapsed_secs = (tm_end.tv_sec - tm_start.tv_sec) +
                 (float)(tm_end.tv_usec - tm_start.tv_usec)/1000000;
  printf("%s: appended %d entries in %g seconds (%g entries/s)\n", argv[0],
         n_entries, elapsed_secs, n_entries/elapsed_secs);

  job_registry_destroy(rha);
  return 0;
}
