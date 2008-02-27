/*
 *  File :     job_registry.c
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  12-Nov-2007 Original release
 *  27-Feb-2008 Added user_prefix to classad printout.
 *              Added job_registry_split_blah_id and its free call.
 *
 *  Description:
 *    File-based container to cache job IDs and statuses to implement
 *    bulk status commands in BLAH.
 *
 *  Copyright (c) 2007 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include <time.h>
#include <errno.h>

#include "job_registry.h"

/*
 * jobregistry_construct_path
 *
 * Assemble dirname and basename of 'path', and an optional numeric
 * argument into a dynamically-allocated string formatted according to
 * 'format'.
 * 
 * @param format Format string including two %s and an optional %d converter
 *               These will be filled with the dirname, basename and numeric
 *               argument (if > 0).
 * @param path   Full file path name
 * @param num    Optional numerical argument. Will be taken into account
 *               only if it is greater than 0.
 *
 * @return       Dynamically allocated string containing the formatting
 *               result. Needs to be free'd.
 *
 */

char *
jobregistry_construct_path(const char *format, const char *path, 
                           unsigned int num)
 {
  char *pcopy1=NULL, *pcopy2=NULL;
  char *basepath, *dirpath;
  char *retpath = NULL;
  unsigned int ntemp, numlen;

  /* Make copies of the path as basename() and dirname() will change them. */
  pcopy1 = strdup(path);
  if (pcopy1 == NULL)
   {
    errno = ENOMEM;
    return NULL;
   }
  pcopy2 = strdup(path);
  if (pcopy2 == NULL)
   {
    errno = ENOMEM;
    free(pcopy1);
    return NULL;
   }
  
  if (num > 0)
   {
    /* Count pid digits */
    ntemp = num; numlen = 1;
    while (ntemp > 0)
     {
      if ((int)(ntemp/=10) > 0) numlen++;
     }
   }
  else numlen = 0;

  basepath = basename(pcopy1);
  dirpath  = dirname(pcopy2);

  retpath = (char *)malloc(strlen(basepath) + strlen(dirpath) + 
                               strlen(format) + numlen); 

  if (retpath == NULL)
   {
    errno = ENOMEM;
    free(pcopy1);
    free(pcopy2);
    return NULL;
   }

  if (num > 0) sprintf(retpath,format,dirpath,basepath,num);
  else         sprintf(retpath,format,dirpath,basepath);
  free(pcopy1);
  free(pcopy2); 

  return retpath;
}

/*
 * job_registry_purge
 *
 * Remove from the registry located in 'path' all entries that were
 * created before 'oldest_creation_date'.
 * This call is meant to be issued before the registry is open and
 * before job_registry_init() is called, as it has the ability to recover 
 * corrupted files if possible. The file will not be scanned and rewritten
 * if the first entry is more recent than 'oldest_creation_date' unless
 * 'force_rewrite' is true. In the latter case the file will be rescanned
 * and rewritten.
 *
 * @param path Path to the job registry file.
 * @param oldest_creation_date Oldest cdate of entries that will be
 *        kept in the registry.
 * @param force_rewrite If this evaluates to true, it will cause the 
 *        registry file to be entirely scanned and rewritten also in case
 *        no purging is needed.
 *
 * @return Less than zero on error. errno is set in case of error.
 *
 */

int 
job_registry_purge(const char *path, time_t oldest_creation_date,
                   int force_rewrite)
{
  FILE *fd,*fdw;
  struct flock wlock;
  char *newreg_path=NULL;
  job_registry_entry first,cur;
  job_registry_recnum_t first_recnum, last_recnum;
  int ret;
  mode_t old_umask;

  fd = fopen(path,"r+");
  if (fd == NULL) return JOB_REGISTRY_FOPEN_FAIL;

  /* Now obtain the requested write lock */

  wlock.l_type = F_WRLCK;
  wlock.l_whence = SEEK_SET;
  wlock.l_start = 0;
  wlock.l_len = 0; /* Lock whole file */
  
  if (fcntl(fileno(fd), F_SETLKW, &wlock) < 0) 
   {
    fclose(fd);
    return JOB_REGISTRY_FLOCK_FAIL;
   }

  if ((ret = job_registry_seek_next(fd,&first)) < 0)
   {
    if (force_rewrite) ftruncate(fileno(fd), 0);
    fclose(fd);
    return JOB_REGISTRY_NO_VALID_RECORD;
   }
  if (ret == 0)
   {
    /* End of file. */
    fclose(fd);
    return JOB_REGISTRY_SUCCESS;
   }
  
  if ( (first.magic_start != JOB_REGISTRY_MAGIC_START) ||
       (first.magic_end   != JOB_REGISTRY_MAGIC_END) )
   {
    errno = ENOMSG;
    fclose(fd);
    return JOB_REGISTRY_CORRUPT_RECORD;
   }

  first_recnum = last_recnum = first.recnum;

  if (!force_rewrite && (first.cdate >= oldest_creation_date))
   {
    /* Nothing to purge. Go home. */
    fclose(fd);
    return JOB_REGISTRY_SUCCESS;
   }

  /* Create purged registry file that will be rotated in place */
  /* of the current registry. */
  newreg_path = jobregistry_construct_path("%s/%s.new.%d", path, getpid());
  if (newreg_path == NULL) return JOB_REGISTRY_MALLOC_FAIL;
  
  /* Make sure the file is group writable. */
  old_umask=umask(S_IWOTH);
  fdw = fopen(newreg_path,"w");
  umask(old_umask);
  if (fdw == NULL)
   {
    fclose(fd);
    free(newreg_path);
    return JOB_REGISTRY_FOPEN_FAIL;
   }

  if (force_rewrite && (first.cdate >= oldest_creation_date))
   {
    /* Write out the first record if needed */
    if (fwrite(&first,sizeof(job_registry_entry),1,fdw) < 1)
     {
      fclose(fd);
      fclose(fdw);
      free(newreg_path);
      return JOB_REGISTRY_FWRITE_FAIL;
     }
   }

  while (!feof(fd))
   {
    if ((ret = job_registry_seek_next(fd,&cur)) < 0)
     {
      fclose(fd);
      fclose(fdw);
      free(newreg_path);
      return JOB_REGISTRY_NO_VALID_RECORD;
     }
    if (ret == 0) break;

    /* Sanitize sequence numbers */
    if (cur.recnum != (last_recnum+1)) cur.recnum = last_recnum + 1;
    last_recnum = cur.recnum;
    if (cur.cdate < oldest_creation_date) continue;

    if (fwrite(&cur,sizeof(job_registry_entry),1,fdw) < 1)
     {
      fclose(fd);
      fclose(fdw);
      free(newreg_path);
      return JOB_REGISTRY_FWRITE_FAIL;
     }
   }

  fclose(fdw);

  if (rename(newreg_path, path) < 0)
   {
    return JOB_REGISTRY_RENAME_FAIL;
   }

  /* Closing fd will release the write lock. */
  fclose (fd);

  free(newreg_path);

  return JOB_REGISTRY_SUCCESS;
}

/*
 * job_registry_init
 *
 * Sets up for access to a job registry file pointed to by 'path'.
 * An entry index is loaded in memory if mode is not set to NO_INDEX.
 *
 * @param path Pathname of the job registry file.
 * @param mode Operation of the index cache: NO_INDEX disables the cache
 *             (job_registry_lookup, job_registry_update and job_registry_get
 *             cannot be used), BY_BATCH_ID uses batch_id as the cache index 
 *             key while BY_BLAH_ID sets blah_id as the cache index key.
 *
 * @return Pointer to a dynamically allocated handle to the job registry.
 *         This needs to be freed via job_registry_destroy.
 */

job_registry_handle *
job_registry_init(const char *path,
                  job_registry_index_mode mode)
{
  job_registry_handle *rha;
  struct stat lst;
  mode_t old_umask;
  FILE *fd;

  rha = (job_registry_handle *)malloc(sizeof(job_registry_handle));
  if (rha == NULL) 
   {
    errno = ENOMEM;
    return NULL;
   }

  rha->firstrec = rha->lastrec = 0;
  rha->entries = NULL;
  rha->n_entries = 0;
  rha->n_alloc = 0;
  rha->mode = mode;

  /* Copy path of file repository */
  rha->path = strdup(path);
  if (rha->path == NULL)
   {
    free(rha);
    errno = ENOMEM;
    return NULL;
   }
  
  /* Create path for lock test file */
  rha->lockfile = jobregistry_construct_path("%s/.%s.locktest",path,0);
  if (rha->lockfile == NULL)
   {
    free(rha->path);
    free(rha);
    errno = ENOMEM;
    return NULL;
   }

  if (stat(rha->lockfile, &lst) < 0)
   {
    if (errno == ENOENT)
     {
      old_umask = umask(S_IWOTH);
      if (creat(rha->lockfile,0664) < 0)
       {
        umask(old_umask);
        free(rha->path);
        free(rha);
        return NULL;
       }
      umask(old_umask);
     }
    else
     {
      free(rha->path);
      free(rha);
      return NULL;
     }
   }

  /* Now read the entire file and cache its contents */
  /* In NO_INDEX mode only firstrec and lastrec will be updated */
  fd = job_registry_open(rha, "r");
  if (fd == NULL)
   {
    /* Make sure the file is group writable. */
    old_umask = umask(S_IWOTH);
    if (errno == ENOENT) fd = job_registry_open(rha, "w+");
    umask(old_umask);
    if (fd == NULL)
     {
      job_registry_destroy(rha);
      return NULL;
     }
   }
  if (job_registry_rdlock(rha, fd) < 0)
   {
    job_registry_destroy(rha);
    return NULL;
   }
  if (job_registry_resync(rha, fd) < 0)
   {
    fclose(fd);
    job_registry_destroy(rha);
    return NULL;
   }
     
  fclose(fd);

  return(rha);
}

/*
 * job_registry_destroy
 *
 * Frees any dynamic content and the job registry handle itself.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init
 */

void 
job_registry_destroy(job_registry_handle *rha)
{
   if (rha->path != NULL) free(rha->path);
   if (rha->lockfile != NULL) free(rha->lockfile);
   rha->n_entries = rha->n_alloc = 0;
   if (rha->entries != NULL) free(rha->entries);
   free(rha);
}

/*
 * job_registry_firstrec
 * 
 * Get the record number of the first record in the open registry file
 * pointed to by fd. 
 * Record numbers are supposed to be in ascending order, with no gaps. 
 *
 * @param fd Stream descriptor of an open registry file.
 *        fd *must* be at least read locked before calling this function. 
 *        The file will be left positioned after the first record.
 *
 * @return Record number in the first record of the file.
 *
 */

job_registry_recnum_t
job_registry_firstrec(FILE *fd)
{
  int ret;
  job_registry_entry first; 
  ret = fseek(fd, 0L, SEEK_SET);
  if (ret < 0) return ret; 

  ret = fread(&first, sizeof(job_registry_entry), 1, fd);
  if (ret < 1) 
   {
    if (feof(fd)) return 0;
    else return JOB_REGISTRY_FREAD_FAIL;
   }

  if ( (first.magic_start != JOB_REGISTRY_MAGIC_START) ||
       (first.magic_end   != JOB_REGISTRY_MAGIC_END) )
   {
    errno = ENOMSG;
    return JOB_REGISTRY_CORRUPT_RECORD;
   }

  return first.recnum;
}

/*
 * job_registry_resync
 *
 * Update the cache inside the job registry handle, if enabled,
 * otherwise just update rha->firstrec and rha->lastrec.
 * Will rescan the entire file in case this was found to be purged.
 * 
 * @param fd Stream descriptor of an open registry file.
 *        fd *must* be at least read locked before calling this function. 
 * @param rha Pointer to a job registry handle returned by job_registry_init
 *
 * @return Less than zero on error. See job_registry.h for error codes.
 *         errno is also set in case of error.
 */

int
job_registry_resync(job_registry_handle *rha, FILE *fd)
{

  job_registry_recnum_t firstrec;
  job_registry_entry *ren;
  job_registry_index *new_entries;
  char *chosen_id;

  firstrec = job_registry_firstrec(fd);

       /* File new or changed? */
  if ( (rha->lastrec == 0 && rha->firstrec == 0) || firstrec != rha->firstrec)
   {
    if (fseek(fd,0L,SEEK_SET) < 0) return JOB_REGISTRY_FSEEK_FAIL;
    if (rha->entries != NULL) free(rha->entries);
    rha->firstrec = 0;
    rha->lastrec = 0;
    rha->n_entries = 0;
    rha->n_alloc = 0;
   }
  else
   {
    /* Move to the last known end of file and keep on reading */
    if (fseek(fd,(long)((rha->lastrec - rha->firstrec + 1)*sizeof(job_registry_entry)),
              SEEK_SET) < 0) return JOB_REGISTRY_FSEEK_FAIL;
   }

  if (rha->mode == NO_INDEX) 
   {
    /* Just figure out the first and last recnum */
    if ((ren = job_registry_get_next(rha, fd)) != NULL)
     {
      if (rha->firstrec == 0) rha->firstrec = ren->recnum;
      free(ren);
      if (fseek(fd,(long)-sizeof(job_registry_entry),SEEK_END) >= 0)
       {
        if ((ren = job_registry_get_next(rha, fd)) != NULL)
         {
          rha->lastrec = ren->recnum;
          free(ren);
         }
        else return JOB_REGISTRY_NO_VALID_RECORD;
       }
     }
    return JOB_REGISTRY_SUCCESS;
   }

  while ((ren = job_registry_get_next(rha, fd)) != NULL)
   {
    if (rha->firstrec == 0) rha->firstrec = ren->recnum;
    if (ren->recnum > rha->lastrec) rha->lastrec = ren->recnum;
    (rha->n_entries)++;
    if (rha->n_entries > rha->n_alloc)
     {
      rha->n_alloc += JOB_REGISTRY_ALLOC_CHUNK;
      new_entries = realloc(rha->entries, 
                            rha->n_alloc * sizeof(job_registry_index));
      if (new_entries == NULL)
       {
        errno = ENOMEM;
        free(ren);
        return JOB_REGISTRY_MALLOC_FAIL;
       }
      rha->entries = new_entries;
     }
    if (rha->mode == BY_BLAH_ID) chosen_id = ren->blah_id;
    else                         chosen_id = ren->batch_id;

    JOB_REGISTRY_ASSIGN_ENTRY((rha->entries[rha->n_entries-1]).id,
                              chosen_id);
            
    (rha->entries[rha->n_entries-1]).recnum = ren->recnum; 
    free(ren);
   }
  job_registry_sort(rha);
  return JOB_REGISTRY_SUCCESS;
}

/*
 * job_registry_sort
 *
 * Will perform a non-recursive quicksort of the registry index.
 * This needs to be called before job_registry_lookup, as the latter
 * assumes the index to be ordered.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init
 *
 * @return Less than zero on error. See job_registry.h for error codes.
 *         errno is also set in case of error.
 */

int
job_registry_sort(job_registry_handle *rha)
{
  /* Non-recursive quicksort of registry data */

  job_registry_sort_state *sst;
  job_registry_index swap;
  int i,k,kp,left,right,size,median;
  char median_id[JOBID_MAX_LEN];
  int n_sorted;

  /* Anything to do ? */
  if (rha->n_entries <= 1) return JOB_REGISTRY_SUCCESS;

  srand(time(0));

  sst = (job_registry_sort_state *)malloc(rha->n_entries * 
              sizeof(job_registry_sort_state));
  if (sst == NULL)
   {
    errno = ENOMEM;
    return JOB_REGISTRY_MALLOC_FAIL;
   }
  
  sst[0] = LEFT_BOUND;
  for (i=1;i<(rha->n_entries - 1);i++) sst[i] = UNSORTED;
  sst[rha->n_entries - 1] = RIGHT_BOUND;

  for (n_sorted = 0; n_sorted < rha->n_entries; )
   {
    for (left=0; left<rha->n_entries; left=right+1)
     {
      if (sst[left] == SORTED) 
       {
        right = left;
        continue;
       }
      else if (sst[left] == LEFT_BOUND)
       {
        /* Find end of current sort partition */
        for (right = left+1; right<rha->n_entries; right++)
         {
          if (sst[right] == RIGHT_BOUND) break;
          else sst[right] = UNSORTED;
         }
        /* Separate entries from 'right' to 'left' and separate them at */
        /* entry 'median'. */
        size = (right - left + 1);
        median = rand()%size + left;
        JOB_REGISTRY_ASSIGN_ENTRY(median_id, rha->entries[median].id);
        for (i = left, k = right; ; i++,k--)
         {
          while (strcmp(rha->entries[i].id,median_id) < 0) i++;
          while (strcmp(rha->entries[k].id,median_id) > 0) k--;
          if (i>=k) break; /* Indices crossed ? */

          /* If we reach here entries 'i' and 'k' need to be swapped. */
          swap = rha->entries[i];
          rha->entries[i] = rha->entries[k];
          rha->entries[k] = swap;
         }
        /* 'k' is a new candidate right bound. 'k+1' a candidate left bound */
        if (k >= left)
         {
          if (sst[k] == LEFT_BOUND)  sst[k] = SORTED, n_sorted++;
          else                       sst[k] = RIGHT_BOUND;
         }
        kp = k+1;
        if ((kp) <= right)
         {
          if (sst[kp] == RIGHT_BOUND) sst[kp] = SORTED, n_sorted++;
          else                        sst[kp] = LEFT_BOUND;
         }
       }
     }
   }
  free(sst);
  return JOB_REGISTRY_SUCCESS;
}

/*
 * job_registry_append
 *
 * Appends an entry to the registry, unless an entry with the same
 * active ID is found there already (in that case will fall through
 * to job_registry_update).
 * Will open, lock and close the registry file.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param entry pointer to a registry entry to append to the registry.
 *        entry->recnum, entry->cdate and entry->mdate will be updated
 *        to current values. entry->magic_start and entry->magic_end will
 *        be appropriately set.
 *
 * @return Less than zero on error. See job_registry.h for error codes.
 *         errno is also set in case of error.
 */
int
job_registry_append(job_registry_handle *rha,
                    job_registry_entry *entry)
{
  job_registry_recnum_t found,curr_recn;
  job_registry_entry last;
  FILE *fd;
  long curr_pos;
  int ret;

  if (rha->mode != NO_INDEX)
   {
    if (rha->mode == BY_BLAH_ID) 
      found = job_registry_lookup(rha, entry->blah_id);
    else
      found = job_registry_lookup(rha, entry->batch_id);
    if (found != 0)
     { 
      return job_registry_update(rha, entry);
     }
   }

  /* Open file, writelock it and append entry */

  fd = job_registry_open(rha,"a+");
  if (fd == NULL) return JOB_REGISTRY_FOPEN_FAIL;

  if (job_registry_wrlock(rha,fd) < 0)
   {
    fclose(fd);
    return JOB_REGISTRY_FLOCK_FAIL;
   }

  /* 'a+' positions the read pointer at the beginning of the file */
  if (fseek(fd, 0L, SEEK_END) < 0)
   {
    fclose(fd);
    return JOB_REGISTRY_FSEEK_FAIL;
   }

  curr_pos = ftell(fd);
  if (curr_pos > 0)
   {
    /* Read in last recnum */
    if (fseek(fd, (long)-sizeof(job_registry_entry), SEEK_CUR) < 0)
     {
      fclose(fd);
      return JOB_REGISTRY_FSEEK_FAIL;
     }
    if (fread(&last, sizeof(job_registry_entry),1,fd) < 1)
     {
      fclose(fd);
      return JOB_REGISTRY_FREAD_FAIL;
     }
    /* Consistency checks */
    if ( (last.magic_start != JOB_REGISTRY_MAGIC_START) ||
         (last.magic_end   != JOB_REGISTRY_MAGIC_END) )
     {
      errno = ENOMSG;
      fclose(fd);
      return JOB_REGISTRY_CORRUPT_RECORD;
     }
    entry->recnum = last.recnum+1;
   }
  else entry->recnum = 1;

  entry->magic_start = JOB_REGISTRY_MAGIC_START;
  entry->magic_end   = JOB_REGISTRY_MAGIC_END;
  entry->reclen = sizeof(job_registry_entry);
  entry->cdate = entry->mdate = time(0);
  
  if (fwrite(entry, sizeof(job_registry_entry),1,fd) < 1)
   {
    fclose(fd);
    return JOB_REGISTRY_FWRITE_FAIL;
   }

  ret = job_registry_resync(rha,fd);
  fclose(fd);
  return ret;
}

/*
 * job_registry_update
 * job_registry_update_op
 *
 * Update an existing entry in the job registry pointed to by rha.
 * Will return JOB_REGISTRY_NOT_FOUND if the entry does not exist.
 * job_registry_update will cause job_registry_update_op to open, (write) lock 
 * and close the registry file, while job_registry_update_op will work
 * on a file that's aleady open and writelocked.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param entry pointer to a registry entry with values to be
 *        updated into the registry.
 *        The values of udate, status, exitcode, exitreason and wn_addr
 *        will be used for the update.
 * @param fd Stream descriptor of an open (for write) and writelocked 
 *        registry file. The file will be opened and closed if fd==NULL.
 *
 * @return Less than zero on error. See job_registry.h for error codes.
 *         errno is also set in case of error.
 */

int
job_registry_update(job_registry_handle *rha,
                    job_registry_entry *entry)
{
  return job_registry_update_op(rha, entry, NULL);
}

int
job_registry_update_op(job_registry_handle *rha,
                       job_registry_entry *entry, FILE *fd)
{
  job_registry_recnum_t found, firstrec, req_recn;
  job_registry_entry old_entry;
  int need_to_fclose = FALSE;
  int need_to_update = FALSE;

  if (rha->mode == NO_INDEX)
    return JOB_REGISTRY_NO_INDEX;
  else if (rha->mode == BY_BLAH_ID) 
    found = job_registry_lookup_op(rha, entry->blah_id, fd);
  else
    found = job_registry_lookup_op(rha, entry->batch_id, fd);
  if (found == 0)
   {
    return JOB_REGISTRY_NOT_FOUND;
   }

  if (fd == NULL)
   {
    /* Open file, writelock it and replace entry */

    fd = job_registry_open(rha,"r+");
    if (fd == NULL) return JOB_REGISTRY_FOPEN_FAIL;

    if (job_registry_wrlock(rha,fd) < 0)
     {
      fclose(fd);
      return JOB_REGISTRY_FLOCK_FAIL;
     }
    need_to_fclose = TRUE;
   }

  firstrec = job_registry_firstrec(fd);
  JOB_REGISTRY_GET_REC_OFFSET(req_recn,found,firstrec)
  
  if (fseek(fd, (long)(req_recn*sizeof(job_registry_entry)), SEEK_SET) < 0)
   {
    if (need_to_fclose) fclose(fd);
    return JOB_REGISTRY_FSEEK_FAIL;
   }
  if (fread(&old_entry, sizeof(job_registry_entry),1,fd) < 1)
   {
    if (need_to_fclose) fclose(fd);
    return JOB_REGISTRY_FREAD_FAIL;
   }
  if (old_entry.recnum != found)
   {
    errno = EBADMSG;
    if (need_to_fclose) fclose(fd);
    return JOB_REGISTRY_BAD_RECNUM;
   }
  if (fseek(fd, (long)(req_recn*sizeof(job_registry_entry)), SEEK_SET) < 0)
   {
    if (need_to_fclose) fclose(fd);
    return JOB_REGISTRY_FSEEK_FAIL;
   }

  /* Update original entry and rewrite it */
  if (strncmp(old_entry.wn_addr, entry->wn_addr, sizeof(old_entry.wn_addr)) != 0)
   {
    JOB_REGISTRY_ASSIGN_ENTRY(old_entry.wn_addr, entry->wn_addr);
    need_to_update = TRUE;
   }
  if (old_entry.status != entry->status)
   {
    old_entry.status = entry->status;
    need_to_update = TRUE;
   }
  if (old_entry.exitcode != entry->exitcode)
   {
    old_entry.exitcode = entry->exitcode;
    need_to_update = TRUE;
   }
  if (old_entry.udate != entry->udate)
   {
    old_entry.udate = entry->udate;
    need_to_update = TRUE;
   }
  if (strncmp(old_entry.exitreason, entry->exitreason, sizeof(old_entry.exitreason)) != 0)
   {
    JOB_REGISTRY_ASSIGN_ENTRY(old_entry.exitreason, entry->exitreason);
    need_to_update = TRUE;
   }

  if (need_to_update)
   {
    old_entry.mdate = time(0);
  
    if (fwrite(&old_entry, sizeof(job_registry_entry),1,fd) < 1)
     {
      if (need_to_fclose) fclose(fd);
      return JOB_REGISTRY_FWRITE_FAIL;
     }
   }

  if (need_to_fclose) fclose(fd);
  return JOB_REGISTRY_SUCCESS;
}

/*
 * job_registry_lookup
 * job_registry_lookup_op
 *
 * Binary search for an entry in the indexed, sorted job registry pointed to by
 * rha. If the entry is not found, a job_registry_resync will be attempted.
 * job_registry_lookup_op can operate on an already open and at least read-
 * locked file.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param id Job id key to be looked up 
 * @param fd Stream descriptor of an open and at least readlocked 
 *        registry file. The file will be opened and closed if 
 *        job_registry_resync is needed and fd==NULL.
 *
 * @return Record number of the found record, or 0 if the record was not found.
 */

job_registry_recnum_t 
job_registry_lookup(job_registry_handle *rha,
                    const char *id)
{
  /* As a resync attempt is performed inside this function, */
  /* the registry should not be open when this function is called. */
  /* Use job_registry_lookup_op if an open file is needed */

  return job_registry_lookup_op(rha, id, NULL);
}

job_registry_recnum_t 
job_registry_lookup_op(job_registry_handle *rha,
                       const char *id, FILE *fd)
{
  /* Binary search in entries */
  int left,right,cur;
  job_registry_recnum_t found=0;
  int cmp;
  int retry;
  int need_to_fclose = FALSE;

  left = 0;
  right = rha->n_entries -1;

  for (retry=0; retry < 2; retry++)
   {
    while (right >= left)
     {
      cur = (right + left) /2;
      cmp = strcmp(rha->entries[cur].id,id);
      if (cmp == 0)
       {
        found = rha->entries[cur].recnum;
        break;
       }
      else if (cmp < 0)
       {
        left = cur+1;
       }
      else
       {
        right = cur-1;
       }
     }
    /* If the entry was not found the first time around, try resyncing once */
    if (found == 0 && retry == 0)
     {
      if (fd == NULL)
       {
        fd = job_registry_open(rha, "r");
        if (fd == NULL) break;
        if (job_registry_rdlock(rha, fd) < 0)
         {
          fclose(fd);
          break;
         }
        need_to_fclose = TRUE;
       }
      if (job_registry_resync(rha, fd) < 0)
       {
        if (need_to_fclose) fclose(fd);
        break;
       }
      if (need_to_fclose) fclose(fd);
     }
    if (found > 0) break;
   }
  return found;
}

/*
 * job_registry_get
 *
 * Search for an entry in the indexed, sorted job registry pointed to by
 * rha and fetch it from the registry file. If the entry is not found, a 
 * job_registry_resync will be attempted.
 * The registry file will be opened, locked and closed as an effect
 * of this operation, so the file should not be open upon entering
 * this function.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param id Job id key to be looked up 
 *
 * @return Dynamically allocated registry entry. Needs to be free'd.
 */

job_registry_entry *
job_registry_get(job_registry_handle *rha,
                 const char *id)
{
  job_registry_recnum_t found, firstrec, req_recn;
  job_registry_entry *entry;
  FILE *fd;

  if (rha->mode == NO_INDEX)
   {
    errno = EINVAL;
    return NULL;
   }

  found = job_registry_lookup(rha, id);
  if (found == 0)
   {
    errno = ENOENT;
    return NULL;
   }

  /* Open file, readlock it and fetch entry */

  fd = job_registry_open(rha,"r");
  if (fd == NULL) return NULL;

  if (job_registry_rdlock(rha,fd) < 0)
   {
    fclose(fd);
    return NULL;
   }

  firstrec = job_registry_firstrec(fd);
  JOB_REGISTRY_GET_REC_OFFSET(req_recn,found,firstrec)
  
  if (fseek(fd, (long)(req_recn*sizeof(job_registry_entry)), SEEK_SET) < 0)
   {
    fclose(fd);
    return NULL;
   }
  
  entry = job_registry_get_next(rha, fd);

  fclose(fd);
  return entry;
}

/*
 * job_registry_open
 *
 * Open a registry file. Just a wrapper around fopen at the time being.
 * May include more operations further on.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param mode fopen mode string.
 *
 * @return stream descriptor of the open file or NULL on error.
 */

FILE *
job_registry_open(const job_registry_handle *rha, const char *mode)
{
  FILE *fd;

  fd = fopen(rha->path, mode);

  return fd;
}

/*
 * job_registry_rdlock
 *
 * Obtain a read lock to sfd, after making sure no write lock request
 * is pending on the same file.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param sfd Stream descriptor of an open (for reading) file to lock.
 * 
 * @return Less than zero on error. See job_registry.h for error codes.
 *         errno is also set in case of error.
 */

int
job_registry_rdlock(const job_registry_handle *rha, FILE *sfd)
{
  int fd, lfd;
  struct flock tlock, rlock;
  int ret;

  fd = fileno(sfd);
  if (fd < 0) return fd; /* sfd is an invalid stream */

  /* First of all, try obtaining a write lock to rha->lockfile */
  /* to make sure no write lock is pending */

  lfd = open(rha->lockfile, O_WRONLY|O_CREAT, 0664); 
  if (lfd < 0) return lfd;

  tlock.l_type = F_WRLCK;
  tlock.l_whence = SEEK_SET;
  tlock.l_start = 0;
  tlock.l_len = 0; /* Lock whole file */

  if ((ret = fcntl(lfd, F_SETLKW, &tlock)) < 0)  return ret;

  /* Close file immediately */

  close(lfd);

  /* Now obtain the requested read lock */

  rlock.l_type = F_RDLCK;
  rlock.l_whence = SEEK_SET;
  rlock.l_start = 0;
  rlock.l_len = 0; /* Lock whole file */
  
  ret = fcntl(fd, F_SETLKW, &rlock);
  return ret;
}

/*
 * job_registry_wrlock
 * 
 * Obtain a write lock to sfd. Also lock rha->lockfile to prevent more
 * read locks from being issued.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param sfd Stream descriptor of an open (for writing) file to lock.
 * 
 * @return Less than zero on error. See job_registry.h for error codes.
 *         errno is also set in case of error.
 */

int
job_registry_wrlock(const job_registry_handle *rha, FILE *sfd)
{
  int fd, lfd;
  struct flock tlock, wlock;
  int ret;

  fd = fileno(sfd);

  /* Obtain and keep a write lock to rha->lockfile */
  /* to prevent new read locks. */

  lfd = open(rha->lockfile, O_WRONLY|O_CREAT, 0664); 
  if (lfd < 0) return lfd;

  tlock.l_type = F_WRLCK;
  tlock.l_whence = SEEK_SET;
  tlock.l_start = 0;
  tlock.l_len = 0; /* Lock whole file */

  if ((ret = fcntl(lfd, F_SETLKW, &tlock)) < 0)  return ret;

  /* Now obtain the requested write lock */

  wlock.l_type = F_WRLCK;
  wlock.l_whence = SEEK_SET;
  wlock.l_start = 0;
  wlock.l_len = 0; /* Lock whole file */
  
  ret = fcntl(fd, F_SETLKW, &wlock);

  /* Release lock on rha->lockfile */

  close(lfd);

  return ret;
}

/*
 * job_registry_get_next
 *
 * Get the registry entry currently pointed to in open stream fd and try
 * making sure it is consistent.
 *
 * @param rha Pointer to a job registry handle returned by job_registry_init.
 * @param fd Open (at least for reading) stream descriptor into a registry file
 *        The stream must be positioned at the beginning of a valid entry,
 *        or an error will be returned. Use job_registry_seek_next is the
 *        stream needs to be positioned.
 *
 * @return Dynamically allocated registry entry. Needs to be free'd.
 */

job_registry_entry *
job_registry_get_next(const job_registry_handle *rha,
                      FILE *fd)
{
  int ret;
  job_registry_entry *result=NULL;
  long curr_pos;
  job_registry_recnum_t curr_recn;

  result = (job_registry_entry *)malloc(sizeof(job_registry_entry));
  if (result == NULL)
   {
    errno = ENOMEM;
    return NULL;
   } 
  
  ret = fread(result, sizeof(job_registry_entry), 1, fd);
  if (ret < 1)
   {
    free(result);
    return NULL;
   }
  if ( (result->magic_start != JOB_REGISTRY_MAGIC_START) ||
       (result->magic_end   != JOB_REGISTRY_MAGIC_END) )
   {
    errno = ENOMSG;
    free(result);
    return NULL;
   }

  if (rha->firstrec > 0) /* Keep checking file consistency */
                         /* (correspondence of file offset and record num) */
   {
    curr_pos = ftell(fd);
    JOB_REGISTRY_GET_REC_OFFSET(curr_recn,result->recnum,rha->firstrec)
    if (curr_pos != ((curr_recn+1)*sizeof(job_registry_entry)))
     {
      errno = EBADMSG;
      free(result);
      return NULL;
     }
   }
  return result;
}

/*
 * job_registry_seek_next
 *
 * Look for a valid registry entry anywhere starting from the current 
 * position in fd. No consistency check is performed. 
 * This function can be used for file recovery. 
 *
 * @param fd Open (at least for reading) stream descriptor into a registry file.
 * @param result Pointer to an allocated registry entry that will be 
 *        used to store the result of the read.
 *
 * @return Less than zero on error. See job_registry.h for error codes.
 *         errno is also set in case of error.
 */

int
job_registry_seek_next(FILE *fd, job_registry_entry *result)
{
  int ret;
  
  ret = fread(result, sizeof(job_registry_entry), 1, fd);
  if (ret < 1)
   {
    if (feof(fd)) return 0;
    else          return JOB_REGISTRY_FREAD_FAIL;
   }

  while ( (result->magic_start != JOB_REGISTRY_MAGIC_START) ||
          (result->magic_end   != JOB_REGISTRY_MAGIC_END) )
   {
    /* Move 1 byte ahead */
    ret = fseek(fd, (long)(-sizeof(job_registry_entry)+1), SEEK_CUR);
    if (ret < 0)
     {
      return JOB_REGISTRY_FSEEK_FAIL;
     }

    ret = fread(result, sizeof(job_registry_entry), 1, fd);
    if (ret < 1)
     {
      if (feof(fd)) return 0;
      else          return JOB_REGISTRY_FREAD_FAIL;
     }
   }

  return 1;
}

/*
 * job_registry_entry_as_classad
 *
 * Create a classad (in standard string representation)
 * including the attributes of the supplied registry entry.
 *
 * @param entry Job registry entry to be formatted as classad.
 *
 * @return Dynamically-allocated string containing the classad
 *         rappresentation of entry.
 */

#define JOB_REGISTRY_APPEND_ATTRIBUTE(format,attribute) \
    fmt_extra = (format); \
    esiz = snprintf(NULL, 0, fmt_extra, (attribute)) + 1; \
    new_extra_attrs = (char *)realloc(extra_attrs, extra_attrs_size + esiz); \
    if (new_extra_attrs == NULL) \
     { \
      if (extra_attrs != NULL) free(extra_attrs); \
      return NULL; \
     } \
    need_to_free_extra_attrs = TRUE; \
    extra_attrs = new_extra_attrs; \
    snprintf(extra_attrs+extra_attrs_size, esiz, fmt_extra, (attribute)); \
    extra_attrs_size += (esiz-1); 

char *
job_registry_entry_as_classad(const job_registry_entry *entry)
{
  char *fmt_base = "[ BatchJobId=\"%s\"; JobStatus=%d; BlahJobId=\"%s\"; "
                   "CreateTime=%u; ModifiedTime=%u; UserTime=%u; "
                   "SubmitterUid=%d; %s]";
  char *result, *fmt_extra, *extra_attrs=NULL, *new_extra_attrs;
  char *extra_attrs_append;
  int extra_attrs_size = 0;
  int need_to_free_extra_attrs = FALSE;
  int esiz,fsiz;

  if ((entry->wn_addr != NULL) && (strlen(entry->wn_addr) > 0)) 
   { 
    JOB_REGISTRY_APPEND_ATTRIBUTE("WorkerNode=\"%s\"; ",entry->wn_addr);
   }
  if (entry->exitcode > 0)
   {
    JOB_REGISTRY_APPEND_ATTRIBUTE("ExitCode=%d; ",entry->exitcode);
   }
  if ((entry->exitreason != NULL) && (strlen(entry->exitreason) > 0)) 
   { 
    JOB_REGISTRY_APPEND_ATTRIBUTE("ExitReason=\"%s\"; ",entry->exitreason);
   }
  if ((entry->user_prefix != NULL) && (strlen(entry->user_prefix) > 0)) 
   { 
    JOB_REGISTRY_APPEND_ATTRIBUTE("UserPrefix=\"%s\"; ",entry->user_prefix);
   }

  if (extra_attrs == NULL) 
   {
    extra_attrs = "";
    need_to_free_extra_attrs = FALSE;
   }

  fsiz = snprintf(NULL, 0, fmt_base, 
                  entry->batch_id, entry->status, entry->blah_id,
                  entry->cdate, entry->mdate, entry->udate, entry->submitter,
                  extra_attrs) + 1;

  result = (char *)malloc(fsiz);
  if (result)
    snprintf(result, fsiz, fmt_base,
             entry->batch_id, entry->status, entry->blah_id,
             entry->cdate, entry->mdate, entry->udate, entry->submitter,
             extra_attrs);

  if (need_to_free_extra_attrs) free(extra_attrs);

  return result;
}

/*
 * job_registry_split_blah_id
 *
 * Return a structure (to be freed with job_registry_free_split_id)
 * filled with dynamically allocated copies of the various parts
 * of the BLAH ID:
 *  - lrms: the part up to and excluding the first slash ('/')
 *  - script_id: the part following and excluding the first slash 
 *  - proxy_id: the part between the first and the second slash 
 *
 * @param id BLAH job ID string.
 *
 * @return Pointer to a job_registry_split_id structure containing the split id
 *         or NULL if no slash was found in the ID or malloc failed.
 *         The resulting pointer has to be freed via job_registry_free_split_id.
 */

job_registry_split_id *
job_registry_split_blah_id(const char *bid)
 {
  char *firsts, *seconds;
  int fsl, ssl, psl;
  job_registry_split_id *ret;

  firsts = strchr(bid, '/');

  if (firsts == NULL) return NULL;

  ret = (job_registry_split_id *)malloc(sizeof(job_registry_split_id));
  if (ret == NULL) return NULL;

  seconds = strchr(firsts+1, '/');
  if (seconds == NULL) seconds = bid+strlen(bid);

  fsl = (int)(firsts - bid);
  ssl = strlen(bid)-fsl-1;
  psl = (int)(seconds - firsts) - 1;

  ret->lrms      = (char *)malloc(fsl + 1);
  ret->script_id = (char *)malloc(ssl + 1);
  ret->proxy_id  = (char *)malloc(ssl + 1);

  if (ret->lrms == NULL || ret->script_id == NULL || ret->proxy_id == NULL)
   {
    job_registry_free_split_id(ret);
    return NULL;
   }

  memcpy(ret->lrms,      bid, fsl);
  memcpy(ret->script_id, firsts+1, ssl);
  memcpy(ret->proxy_id,  firsts+1, psl);

  (ret->lrms)     [fsl] = '\000';
  (ret->script_id)[ssl] = '\000';
  (ret->proxy_id) [psl] = '\000';

  return ret;
 }

/*
 * job_registry_free_split_id
 *
 * Frees a job_registry_split_id with all its dynamic contents.
 *
 * @param spid Pointer to a job_registry_split_id returned by 
 *             job_registry_split_blah_id.
 */

void 
job_registry_free_split_id(job_registry_split_id *spid)
 {
   if (spid == NULL) return;

   if (spid->lrms != NULL)      free(spid->lrms);
   if (spid->script_id != NULL) free(spid->script_id);
   if (spid->proxy_id != NULL)  free(spid->proxy_id);

   free(spid);
 }
