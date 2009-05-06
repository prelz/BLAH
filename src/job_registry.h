/*
 *  File :     job_registry.h
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  12-Nov-2007 Original release
 *  27-Feb-2008 Added user_prefix at CREAM's request.
 *              Added job_registry_split_blah_id and its free call.
 *   3-Mar-2008 Added non-privileged updates to fit CREAM's file and process
 *              ownership model.
 *   8-Jan-2009 Added job_registry_update_select call.
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
#include <string.h>
#include <time.h>
#include "blahpd.h"

typedef uint32_t job_registry_recnum_t;
typedef uint32_t job_registry_update_bitmask_t;

#define JOB_REGISTRY_MAX_RECNUM 0xffffffff
/* Allow record numbers to roll over */
#define JOB_REGISTRY_GET_REC_OFFSET(req_recn,found,firstrec) \
  if ((found) >= (firstrec)) (req_recn) = (found) - (firstrec); \
  else (req_recn) = JOB_REGISTRY_MAX_RECNUM - (firstrec) + (found);

#define JOB_REGISTRY_MAX_EXITREASON 120
#define JOB_REGISTRY_MAX_USER_PREFIX 32
#define JOB_REGISTRY_MAX_PROXY_LINK  40
#define JOB_REGISTRY_MAX_SUBJECTLIST_LINE 512

typedef uint32_t job_registry_entry_magic_t;
typedef struct job_registry_entry_s
 {
   job_registry_entry_magic_t magic_start; 
   uint32_t     reclen;
   job_registry_recnum_t recnum;
   uid_t        submitter;
   time_t       cdate;
   time_t       mdate;
   time_t       udate;
   char         blah_id[JOBID_MAX_LEN];
   char         batch_id[JOBID_MAX_LEN];
   job_status_t status; 
   int32_t          exitcode;
   char         exitreason[JOB_REGISTRY_MAX_EXITREASON];
   char         wn_addr[40]; /* Accommodates IPV6 addresses */
   char         user_prefix[JOB_REGISTRY_MAX_USER_PREFIX];
   char         proxy_link[JOB_REGISTRY_MAX_PROXY_LINK];
   int32_t      renew_proxy; 
#define                 JOB_REGISTRY_ENTRY_UPDATE_2 40
   char         subject_hash[JOB_REGISTRY_ENTRY_UPDATE_2]; 
   job_registry_entry_magic_t magic_end; 
 } job_registry_entry;

/* The job_registry_entry struct can be expanded by *adding* fields         */
/* before magic_end. When this is done, remember to update the following    */
/* array definition: (zero-terminated array)  */

#define JOB_REGISTRY_ENTRY_UPDATE_1 60

#define N_JOB_REGISTRY_ALLOWED_ENTRY_SIZE_INCS 4
#define JOB_REGISTRY_ALLOWED_ENTRY_SIZE_INCS  \
      { sizeof(job_registry_entry) - \
        2*sizeof(job_registry_entry_magic_t) - \
        JOB_REGISTRY_ENTRY_UPDATE_1 - \
        JOB_REGISTRY_ENTRY_UPDATE_2, \
        JOB_REGISTRY_ENTRY_UPDATE_1, \
        JOB_REGISTRY_ENTRY_UPDATE_2, 0 };

#define JOB_REGISTRY_ASSIGN_ENTRY(dest,src) \
  (dest)[sizeof(dest)-1]='\000'; \
  strncpy((dest),(src),sizeof(dest)); 

typedef struct job_registry_index_s
 {
   char         id[JOBID_MAX_LEN];
   job_registry_recnum_t recnum;
 } job_registry_index;

typedef enum job_registry_index_mode_e
 {
   NO_INDEX,
   BY_BLAH_ID,
   BY_BATCH_ID,
   NAMES_ONLY
 } job_registry_index_mode;

typedef struct job_registry_handle_s
 {
   uint32_t firstrec;
   uint32_t lastrec;
   char *path;
   char *lockfile;
   char *subjectfile;
   char *npudir;
   char *proxydir;
   char *subjectlist;
   char *npusubjectlist;
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

typedef struct job_registry_split_id_s
 {
   char *lrms;
   char *script_id;
   char *proxy_id;
 } job_registry_split_id;

typedef struct job_registry_hash_store_s
 {
   char **data;
   int n_data;
 } job_registry_hash_store;

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
#define JOB_REGISTRY_OPENDIR_FAIL    -15 
#define JOB_REGISTRY_HASH_EXISTS     -16 

#define JOB_REGISTRY_TEST_FILE "/tmp/test_reg.bjr"
#define JOB_REGISTRY_REGISTRY_NAME "registry"
#define JOB_REGISTRY_ALLOC_CHUNK     20

char *jobregistry_construct_path(const char *format, const char *path,
                                 unsigned int num);
size_t job_registry_probe_next_record(FILE *fd, job_registry_entry *en);
int job_registry_update_reg(const job_registry_handle *rha, const char *old_path);
int job_registry_purge(const char *path, time_t oldest_creation_date,
                       int force_rewrite);
job_registry_handle *job_registry_init(const char *path, 
                                       job_registry_index_mode mode);
void job_registry_destroy(job_registry_handle *rhandle);
job_registry_recnum_t job_registry_firstrec(FILE *fd);
int job_registry_resync(job_registry_handle *rhandle, FILE *fd);
int job_registry_sort(job_registry_handle *rhandle);
job_registry_recnum_t job_registry_get_recnum(const job_registry_handle *rha,
                                              const char *id);
job_registry_recnum_t job_registry_lookup(job_registry_handle *rhandle,
                                          const char *id);
job_registry_recnum_t job_registry_lookup_op(job_registry_handle *rhandle,
                                          const char *id, FILE *fd);
int job_registry_append(job_registry_handle *rhandle, 
                        job_registry_entry *entry);
int job_registry_append_op(job_registry_handle *rhandle, 
                        job_registry_entry *entry, FILE *fd, time_t now);
FILE* job_registry_get_new_npufd(job_registry_handle *rha);
int job_registry_append_nonpriv(job_registry_handle *rha,
                                job_registry_entry *entry);
int job_registry_merge_pending_nonpriv_updates(job_registry_handle *rha,
                                               FILE *fd);

/* Bitmask field definition for job_registry_update_select */
#define JOB_REGISTRY_UPDATE_ALL  0xffffffff
#define JOB_REGISTRY_UPDATE_WN_ADDR    0x01
#define JOB_REGISTRY_UPDATE_STATUS     0x02
#define JOB_REGISTRY_UPDATE_EXITCODE   0x04
#define JOB_REGISTRY_UPDATE_UDATE      0x08
#define JOB_REGISTRY_UPDATE_EXITREASON 0x10

int job_registry_update_select(job_registry_handle *rhandle, 
                        job_registry_entry *entry,
                        job_registry_update_bitmask_t upbits);
int job_registry_update(job_registry_handle *rhandle, 
                        job_registry_entry *entry);
int job_registry_update_op(job_registry_handle *rhandle, 
                        job_registry_entry *entry, FILE *fd,
                        job_registry_update_bitmask_t upbits);
job_registry_entry *job_registry_get(job_registry_handle *rhandle,
                                     const char *id);
FILE *job_registry_open(job_registry_handle *rhandle, const char *mode);
int job_registry_rdlock(const job_registry_handle *rhandle, FILE *sfd);
int job_registry_wrlock(const job_registry_handle *rhandle, FILE *sfd);
job_registry_entry *job_registry_get_next(const job_registry_handle *rhandle,
                                          FILE *fd);
int job_registry_seek_next(FILE *fd, job_registry_entry *result);
char *job_registry_entry_as_classad(const job_registry_handle *rha,
                                    const job_registry_entry *entry);
job_registry_split_id *job_registry_split_blah_id(const char *bid);
void job_registry_free_split_id(job_registry_split_id *spid);
int job_registry_set_proxy(const job_registry_handle *rha,
                           job_registry_entry *en, char *proxy);
char *job_registry_get_proxy(const job_registry_handle *rha,
                             const job_registry_entry *en);
int job_registry_unlink_proxy(const job_registry_handle *rha,
                              job_registry_entry *en);
void job_registry_compute_subject_hash(job_registry_entry *en,
                                       const char *subject);
int job_registry_record_subject_hash(const job_registry_handle *rha,
                                     const char *hash,
                                     const char *subject,
                                     int nonpriv_allowed);
char *job_registry_lookup_subject_hash(const job_registry_handle *rha,
                                       const char *hash);
job_registry_entry *job_registry_get_next_hash_match(
                                 const job_registry_handle *rha,
                                 FILE *fd, const char *hash);
int job_registry_store_hash(job_registry_hash_store *hst,
                            const char *hash);
int job_registry_lookup_hash(const job_registry_hash_store *hst,
                             const char *hash, int *loc);
void job_registry_free_hash_store(job_registry_hash_store *hst);
int job_registry_purge_subject_hash_list(const job_registry_handle *rha,
                                         const job_registry_hash_store *hst);


#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#endif /*defined __JOB_REGISTRY_H__*/
