/*
 *  File :     blah_job_registry_scan_by_subject.c
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *   5-May-2009 Original release
 *
 *  Description:
 *   Executable to look up for entries in the BLAH job registry
 *   that match a given proxy subject, and print them in variable
 *   formats.
 *
 *  Copyright (c) 2007 Istituto Nazionale di Fisica Nucleare (INFN).
 *   All rights reserved.
 *   See http://grid.infn.it/grid/license.html for license details.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "classad_c_helper.h"
#include "job_registry.h"
#include "config.h"

void
undo_escapes(char *string)
{
  char *rp,*wp,ec;
  char nc2,nc3;

  if (string == NULL) return;

  for (rp = string, wp = string; (*rp != '\000'); rp++, wp++)
   {
    if (*rp == '\\')
     {
      ec = *(rp+1);
      switch (ec)
       {
        case 'a': *wp = '\a'; rp++; break;
        case 'b': *wp = '\b'; rp++; break;
        case 'f': *wp = '\f'; rp++; break;
        case 'n': *wp = '\n'; rp++; break;
        case 'r': *wp = '\r'; rp++; break;
        case 't': *wp = '\t'; rp++; break;
        case 'v': *wp = '\v'; rp++; break;
        case '\\': *wp = '\\'; rp++; break;
        case '\?': *wp = '\?'; rp++; break;
        case '\'': *wp = '\''; rp++; break;
        case '\"': *wp = '\"'; rp++; break;
        default: 
          /* Do we have a numeric escape sequence ? */
          if ( (nc2 = *(rp+2)) != '\000' &&
               (nc3 = *(rp+3)) != '\000' )
           {

            if (ec == 'x')
             {
              /* hex char */
              if (nc2 >= 'A' && nc2 <= 'F') nc2 -= ('A' - '9' - 1);
              if (nc2 >= 'a' && nc2 <= 'f') nc2 -= ('a' - '9' - 1);
              if (nc3 >= 'A' && nc3 <= 'F') nc3 -= ('A' - '9' - 1);
              if (nc3 >= 'a' && nc3 <= 'f') nc3 -= ('a' - '9' - 1);
              nc2 -= '0';
              nc3 -= '0';
              if (nc2 >= 0 && nc2 <= 15 && nc3 >= 0 && nc3 <= 15)
               {
                *wp = (nc2*16 + nc3);
                rp+=3; 
                break;
               }
             }
            if (ec >= '0' && ec <= '9' && nc2 >= '0' && nc2 <= '9' &&
                                          nc3 >= '0' && nc3 <= '9')
             {
              /* Octal char */
              ec  -= '0';
              nc2 -= '0';
              nc3 -= '0';
              *wp = (ec * 64 + nc2 * 8 + nc3);
              rp+=3; 
              break;
             }
           }
          if (ec == '0') *wp = '\0'; rp++; break;
          /* True 'default' case: Nothing could be figured out */
          if (rp != wp) *wp = *rp;
       } 
     }
    else if (rp != wp) *wp = *rp;
   }
  /* Terminate the string */
  if (rp != wp) *wp = '\000';
}

#define USAGE_STRING "ERROR Usage: %s (<-s (proxy subject)>|<-h (proxy subject hash)> \"Optional arg1 format\" arg1 \"Optional arg2 format\" arg2, etc.\n"
int
main(int argc, char *argv[])
{
  char *registry_file=NULL, *registry_file_env=NULL;
  int need_to_free_registry_file = FALSE;
  const char *default_registry_file = "blah_job_registry.bjr";
  char *my_home;
  job_registry_entry hen, *ren;
  char *cad;
  classad_context pcad;
  config_handle *cha;
  config_entry *rge;
  job_registry_handle *rha;
  FILE *fd;
  char *arg = "";
  char *looked_up_subject = NULL;
  char *lookup_subject = NULL;
  char *lookup_hash;
  int format_args = -1;
  int ifr;
 
  if (argc < 2)
   {
    fprintf(stderr,USAGE_STRING,argv[0]);
    return 1;
   }

  /* Look up for command line switches */
  if (argv[1][0] != '-')
   {
    fprintf(stderr,USAGE_STRING,argv[0]);
    return 1;
   }

  if (strlen(argv[1]) > 2)
   {
    arg = argv[1] + 2;
    if (argc > 2)
     {
      format_args = 2;
     }
   }
  else if (argc > 2)
   {
    arg = argv[2]; 
    if (argc > 3)
     {
      format_args = 3;
     }
   }

  if (strlen(arg) <= 0)
   {
    fprintf(stderr,USAGE_STRING,argv[0]);
    return 1;
   }

  switch (argv[1][1])
   {
    case 'h':
      lookup_hash = arg;
      break;
    case 's':
      job_registry_compute_subject_hash(&hen, arg);
      lookup_subject = arg;
      lookup_hash = hen.subject_hash;
      break;
    default:
      fprintf(stderr,USAGE_STRING,argv[0]);
      return 1;
   }
    
  cha = config_read(NULL); /* Read config from default locations. */
  if (cha != NULL)
   {
    rge = config_get("job_registry",cha);
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
      fprintf(stderr,"ERROR %s: Out of memory.\n",argv[0]);
      if (cha != NULL) config_free(cha);
      return 1;
     }
   }

  rha=job_registry_init(registry_file, NO_INDEX);

  if (rha == NULL)
   {
    fprintf(stderr,"ERROR %s: error initialising job registry: %s\n",argv[0],
            strerror(errno));
    if (cha != NULL) config_free(cha);
    if (need_to_free_registry_file) free(registry_file);
    return 2;
   }

  /* Filename is stored in job registry handle. - Don't need these anymore */
  if (cha != NULL) config_free(cha);
  if (need_to_free_registry_file) free(registry_file);

  looked_up_subject = job_registry_lookup_subject_hash(rha, lookup_hash);
  if (looked_up_subject == NULL)
   {
    fprintf(stderr,"%s: Hash %s is not found in registry %s.\n",argv[0],
            lookup_hash, rha->path);
    job_registry_destroy(rha);
    return 5;
   } else {
    if ((lookup_subject != NULL) && 
        (strcmp(looked_up_subject, lookup_subject) != 0))
     {
      fprintf(stderr, "%s: Warning: cached subject (%s) differs from the requested subject (%s)\n", argv[0], looked_up_subject, lookup_subject);
     }
    free(looked_up_subject);
   }

  fd = job_registry_open(rha, "r");
  if (fd == NULL)
   {
    fprintf(stderr,"ERROR %s: error opening job registry: %s\n",argv[0],
            strerror(errno));
    job_registry_destroy(rha);
    return 2;
   }

  if (job_registry_rdlock(rha, fd) < 0)
   {
    fprintf(stderr,"ERROR %s: error read-locking job registry: %s\n",argv[0],
            strerror(errno));
    fclose(fd);
    job_registry_destroy(rha);
    return 2;
   }

  if (format_args > 0)
   {
    for (ifr = format_args; ifr < argc; ifr+=2) undo_escapes(argv[ifr]);
   }

  while ((ren = job_registry_get_next_hash_match(rha, fd, lookup_hash)) != NULL)
   {
    cad = job_registry_entry_as_classad(rha, ren);
    if (cad != NULL)
     {
      if (format_args <= 0)
       {
        printf("%s\n",cad);
       }
      else
       {
        if ((pcad = classad_parse(cad)) == NULL)
         {
          fprintf(stderr,"ERROR %s: Cannot parse classad %s.\n",argv[0],cad);
          free(cad);
          free(ren);
          continue;
         }
        for (ifr = format_args; ifr < argc; ifr+=2)
         {
          if ((ifr+1) >= argc)
           {
            printf(argv[ifr]);
           }
          else
           {
            if (classad_get_dstring_attribute(pcad, argv[ifr+1], &arg) == C_CLASSAD_NO_ERROR)
             {
              printf(argv[ifr],arg);
              free(arg);
             }
           }
         }
        classad_free(pcad); 
       }
      free(cad);
     }
    else 
     {
      fprintf(stderr,"ERROR %s: Out of memory.\n",argv[0]);
      free(ren);
      job_registry_destroy(rha);
      return 3;
     }
    free(ren);
   }

  fclose(fd);

  job_registry_destroy(rha);
  return 0;
}
