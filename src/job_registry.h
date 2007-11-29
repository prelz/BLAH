/*
 *  File :     job_registry.h
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  12-Nov-2007 Original release
 *
 *  Description:
 *    Prototypes of functions defined in job_registry.c
 *
 *  Copyright (c) 2007 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#ifndef __JOB_REGISTRY_H__
#define __JOB_REGISTRY_H__

#define JOB_REGISTRY_MAGIC_START 0x49474542
#define JOB_REGISTRY_MAGIC_END   0x20444e45

#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include "blahpd.h"

typedef uint32_t job_registry_recnum_t;
#define JOB_REGISTRY_MAX_RECNUM 0xffffffff
/* Allow record numbers to roll over */
#define JOB_REGISTRY_GET_REC_OFFSET(req_recn,found,firstrec) \
  if ((found) >= (firstrec)) (req_recn) = (found) - (firstrec); \
  else (req_recn) = JOB_REGISTRY_MAX_RECNUM - (firstrec) + (found);

#define JOB_REGISTRY_MAX_EXITREASON 120

typedef struct job_registry_entry_s
 {
   uint32_t     magic_start; 
   uint32_t     reclen;
   job_registry_recnum_t recnum;
   uid_t        submitter;
   time_t       cdate;
   time_t       mdate;
   time_t       udate;
   char         blah_id[JOBID_MAX_LEN];
   char         batch_id[JOBID_MAX_LEN];
   job_status_t status; 
   int          exitcode;
   char         exitreason[JOB_REGISTRY_MAX_EXITREASON];
   char         wn_addr[40]; /* Accommodates IPV6 addresses */
   uint32_t     magic_end; 
 } job_registry_entry;

#define JOB_REGISTRY_ASSIGN_ENTRY(dest,src) \
  (dest)[sizeof(dest)-1]='\000'; \
  memcpy((dest),(src),sizeof(dest)); 

typedef struct job_registry_index_s
 {
   char         id[JOBID_MAX_LEN];
   job_registry_recnum_t recnum;
 } job_registry_index;

typedef enum job_registry_index_mode_e
 {
   NO_INDEX,
   BY_BLAH_ID,
   BY_BATCH_ID
 } job_registry_index_mode;

typedef struct job_registry_handle_s
 {
   uint32_t firstrec;
   uint32_t lastrec;
   char *path;
   char *lockfile;
   job_registry_index *entries;
   int n_entries;
   int n_alloc;
   job_registry_index_mode mode;
 } job_registry_handle;

typedef enum job_registry_sort_state_e
 {
   UNSORTED,
   LEFT_BOUND,
   RIGHT_BOUND,
   SORTED
 } job_registry_sort_state;

#define JOB_REGISTRY_SUCCESS          0
#define JOB_REGISTRY_FAIL            -1 
#define JOB_REGISTRY_NO_INDEX        -2 
#define JOB_REGISTRY_NOT_FOUND       -3 
#define JOB_REGISTRY_FOPEN_FAIL      -4 
#define JOB_REGISTRY_FLOCK_FAIL      -5 
#define JOB_REGISTRY_FSEEK_FAIL      -6 
#define JOB_REGISTRY_FREAD_FAIL      -7 
#define JOB_REGISTRY_FWRITE_FAIL     -8 
#define JOB_REGISTRY_RENAME_FAIL     -9 
#define JOB_REGISTRY_MALLOC_FAIL     -10 
#define JOB_REGISTRY_NO_VALID_RECORD -11 
#define JOB_REGISTRY_CORRUPT_RECORD  -12 
#define JOB_REGISTRY_BAD_POSITION    -13 
#define JOB_REGISTRY_BAD_RECNUM      -14 

#define JOB_REGISTRY_TEST_FILE "/tmp/test_reg.bjr"
#define JOB_REGISTRY_ALLOC_CHUNK     20

char *jobregistry_construct_path(const char *format, const char *path,
                                 unsigned int num);
int job_registry_purge(const char *path, time_t oldest_creation_date,
                       int force_rewrite);
job_registry_handle *job_registry_init(const char *path, 
                                       job_registry_index_mode mode);
void job_registry_destroy(job_registry_handle *rhandle);
job_registry_recnum_t job_registry_firstrec(FILE *fd);
int job_registry_resync(job_registry_handle *rhandle, FILE *fd);
int job_registry_sort(job_registry_handle *rhandle);
job_registry_recnum_t job_registry_lookup(job_registry_handle *rhandle,
                                          const char *id);
job_registry_recnum_t job_registry_lookup_op(job_registry_handle *rhandle,
                                          const char *id, FILE *fd);
int job_registry_append(job_registry_handle *rhandle, 
                        job_registry_entry *entry);
int job_registry_update(job_registry_handle *rhandle, 
                        job_registry_entry *entry);
int job_registry_update_op(job_registry_handle *rhandle, 
                        job_registry_entry *entry, FILE *fd);
job_registry_entry *job_registry_get(job_registry_handle *rhandle,
                                     const char *id);
FILE *job_registry_open(const job_registry_handle *rhandle, const char *mode);
int job_registry_rdlock(const job_registry_handle *rhandle, FILE *sfd);
int job_registry_wrlock(const job_registry_handle *rhandle, FILE *sfd);
job_registry_entry *job_registry_get_next(const job_registry_handle *rhandle,
                                          FILE *fd);
int job_registry_seek_next(FILE *fd, job_registry_entry *result);
char *job_registry_entry_as_classad(const job_registry_entry *entry);

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#endif /*defined __JOB_REGISTRY_H__*/
