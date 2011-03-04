/*
 *  File :     config.c
 *
 *
 *  Author :   Francesco Prelz ($Author: fprelz $)
 *  e-mail :   "francesco.prelz@mi.infn.it"
 *
 *  Revision history :
 *  23-Nov-2007 Original release
 *  24-Apr-2009 Added parsing of shell arrays.
 *
 *  Description:
 *    Small library for access to the BLAH configuration file.
 *
 *  Copyright (c) Members of the EGEE Collaboration. 2007-2010. 
 *
 *    See http://www.eu-egee.org/partners/ for details on the copyright
 *    holders.  
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License"); 
 *    you may not use this file except in compliance with the License. 
 *    You may obtain a copy of the License at 
 *  
 *        http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *    Unless required by applicable law or agreed to in writing, software 
 *    distributed under the License is distributed on an "AS IS" BASIS, 
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *    See the License for the specific language governing permissions and 
 *    limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>

#include "blahpd.h"
#include "config.h"

static int
config_parse_array_values(config_entry *en)
 {
  if (en == NULL) return -1;
  if (en->value == NULL) return -1;

  /* Arrays are dumped in the format foo=([0]="bar1" [1]="bar2") */
  if ((en->value[0] == '(') && (en->value[strlen(en->value)-1] == ')'))
   {
    char *val_regex = "\\[[^]]+\\] *= *\"((\\\\\"|[^\"])*)\""; 
    regex_t regbuf;
    char *match_start = &(en->value[1]);
    regmatch_t pmatch[2];
    int value_len;
    char **new_values;

    if (regcomp(&regbuf, val_regex, REG_EXTENDED) != 0) return -1;

    while (regexec(&regbuf, match_start, 2, pmatch, 0) == 0)
     {
      value_len = pmatch[1].rm_eo - pmatch[1].rm_so;
      new_values = (char **)realloc(en->values, sizeof(char *)*(en->n_values+1));
      if (new_values == NULL) return -1;
      en->values = new_values;
      en->values[en->n_values] = (char *)malloc(value_len+1);
      if (en->values[en->n_values] == NULL) return -1; 
      memcpy(en->values[en->n_values],
             match_start + pmatch[1].rm_so, 
             value_len);
      en->values[en->n_values][value_len] = '\000';
      en->n_values++;
      match_start += pmatch[0].rm_eo;
     }
   }
 }

config_handle *
config_read(const char *ipath)
 {
  char *path;
  char *install_location=NULL;
  char *line=NULL,*new_line=NULL;
  char *cur;
  char *key_start, *key_end, *val_start, *val_end;
  int key_len, val_len;
  char *eq_pos;
  config_handle *rha;
  config_entry *c_tail = NULL;
  config_entry *found,*new_entry=NULL;
  char *set_command=NULL;
  const char *set_command_format = ". %s; set";
  int set_command_size;
  int line_len = 0;
  int line_alloc = 0;
  int c;
  const int line_alloc_chunk = 128;
  FILE *test,*cf;
  config_entry *bp;

  if ((install_location = getenv("BLAHPD_LOCATION")) == NULL)
   {
    install_location = getenv("GLITE_LOCATION");
    if (install_location == NULL) install_location = DEFAULT_GLITE_LOCATION;
   }

  if (ipath == NULL)
   {
    /* Read from default path. */
    path = getenv("BLAHPD_CONFIG_LOCATION");
    if (path == NULL)
     {
      path = (char *)malloc(strlen(CONFIG_FILE_BASE)+strlen(install_location)+6);
      if (path == NULL) return NULL;
      sprintf(path,"%s/etc/%s",install_location,CONFIG_FILE_BASE);
      test = fopen(path, "r");
      /* Last resort if file cannot be read from. */
      if (test == NULL) sprintf(path,"/etc/%s",CONFIG_FILE_BASE);
      else fclose(test);
     }
    else
     {
      /* Make a malloc'ed copy */
      path = strdup(path);
      if (path == NULL) return NULL;
     }
   }
  else 
   {
    path = strdup(ipath);
    if (path == NULL) return NULL;
   }

  set_command_size = snprintf(NULL,0,set_command_format,path)+1;
  set_command = (char *)malloc(set_command_size);
  if (set_command == NULL)
   {
    free(path);
    return NULL;
   }
  snprintf(set_command, set_command_size, set_command_format, path);

  cf = popen(set_command, "r");
  if (cf == NULL) 
   {
    free(set_command);
    free(path);
    return NULL;
   }

  free(set_command);

  rha = (config_handle *)malloc(sizeof(config_handle));
  if (rha == NULL)
   {
    fclose(cf);
    free(path);
    return NULL;
   }

  rha->config_path = path;
  rha->list = NULL;
  if (install_location != NULL) rha->install_path = strdup(install_location);
  if (rha->install_path == NULL)
   {
    /* Out of memory */
    fclose(cf);
    config_free(rha);
    return NULL;
   }
  rha->bin_path = NULL; /* May be filled out of config file contents. */

  line_alloc = line_alloc_chunk;
  line = (char *)malloc(line_alloc);
  if (line == NULL) 
   {
    fclose(cf);
    config_free(rha);
    return NULL;
   }
  line[0]='\000';

  while (!feof(cf))
   {
    c = fgetc(cf);

    if ((char)c == '\n' || c == EOF)
     {
      /* We have one line. Let's look for a variable assignment. */
      cur = line;
      CONFIG_SKIP_WHITESPACE_FWD(cur); 
      if ((*cur) != '#' && (*cur) != '\000' && (*cur) != '=')
       {
        eq_pos = strchr(cur, '=');
        if (eq_pos != NULL)
         {
          key_start = cur;
          key_end = eq_pos - 1;
               /* No danger to cross back beyond start of string */
               /* There must be non-whitespace in between */
          CONFIG_SKIP_WHITESPACE_BCK(key_end); 
          key_end++; /* Position end pointer after valid section */
          val_start = eq_pos + 1;
          CONFIG_SKIP_WHITESPACE_FWD(val_start);
          val_end = line + line_len - 1;
          if (val_end > val_start) CONFIG_SKIP_WHITESPACE_BCK(val_end);
          /* remove single quotes added around values that contain whitespace */
          if (val_end > val_start && ((*val_start) == '\'') && ((*val_end) == '\'') )
           {
            val_start++;
            val_end--;
           }
          val_end++;
          if (key_end > key_start && val_end > val_start)
           {
            /* Key and value are good. Update or append entry */
            *(key_end) = '\000';
            if ((found = config_get(key_start,rha)) != NULL)
             {
              /* Update value */
              if (found->value != NULL) free(found->value);
              val_len = (int)(val_end - val_start);
              found->value = (char *)malloc(val_len + 1);
              if (found->value == NULL)
               {
                /* Out of memory */
                free(line);
                config_free(rha);
                return NULL;
               }
              memcpy(found->value, val_start, val_len);
              found->value[val_len] = '\000';
             }
            else
             {
              /* Append new entry. */ 
              new_entry = (config_entry *)malloc(sizeof(config_entry));
              if (new_entry == NULL)
               {
                /* Out of memory */
                free(line);
                config_free(rha);
                return NULL;
               }
  
              new_entry->key = new_entry->value = NULL;
              new_entry->n_values = 0;
              new_entry->values = NULL;
              new_entry->next = NULL;
              key_len = (int)(key_end - key_start);
              val_len = (int)(val_end - val_start);
              new_entry->key = (char *)malloc(key_len + 1);
              new_entry->value = (char *)malloc(val_len + 1);
              if (new_entry->key == NULL || new_entry->value == NULL)
               {
                /* Out of memory */
                free(line);
                config_free(rha);
                return NULL;
               }
              memcpy(new_entry->key, key_start, key_len);
              new_entry->key[key_len] = '\000';
              memcpy(new_entry->value, val_start, val_len);
              new_entry->value[val_len] = '\000';
              if (c_tail != NULL) c_tail->next = new_entry;
              if (rha->list == NULL) rha->list = new_entry;
              c_tail = new_entry;
              config_parse_array_values(new_entry);
             }
           }
         }
       }
      /* Line is over. Process next line. */
      if (c == EOF) break;
      line_len = 0;
      line[0]='\000';
      continue;
     } /* End of c=='\n' case */

    line_len++;
    if (line_len >= line_alloc)
     {
      line_alloc += line_alloc_chunk;
      new_line = (char *)realloc(line, line_alloc);
      if (new_line == NULL) break; 
      else line = new_line;
     }
    line[line_len-1] = c;
    line[line_len] = '\000'; /* Keep line null-terminated */
   }
  
  fclose(cf);
  free(line);

  if ((bp = config_get("blah_bin_directory", rha)) != NULL)
   {
    rha->bin_path = strdup(bp->value);
   }
  else
   {
    rha->bin_path = (char *)malloc(strlen(install_location)+5);
    if (rha->bin_path != NULL) sprintf(rha->bin_path,"%s/bin",install_location);
   }
  if (rha->bin_path == NULL)
   {
    /* Out of memory */
    config_free(rha);
    return NULL;
   }

  return rha;
 }

config_entry *
config_get(const char *key, config_handle *handle)
 {
  config_entry *cur;

  for (cur = handle->list; cur != NULL; cur=cur->next)
   {
    if (strcmp(cur->key, key) == 0)
     {
      return cur;
     }
   }
  return NULL;
 }

int 
config_test_boolean(const config_entry *entry)
 {
  int vlen;

  if (entry == NULL) return FALSE;

  vlen = strlen(entry->value);
  /* Numerical value != 0 */
  if (vlen > 0 && atoi(entry->value) > 0) return TRUE;
  /* Case-insensitive 'yes' ? */
  if (vlen >= 3 && ((entry->value)[0] == 'y' || (entry->value)[0] == 'Y') &&
                   ((entry->value)[1] == 'e' || (entry->value)[1] == 'E') &&
                   ((entry->value)[2] == 's' || (entry->value)[2] == 'S')) 
    return TRUE;
  /* Case-insensitive 'false' ? */
  if (vlen >= 4 && ((entry->value)[0] == 't' || (entry->value)[0] == 'T') &&
                   ((entry->value)[1] == 'r' || (entry->value)[1] == 'R') &&
                   ((entry->value)[2] == 'u' || (entry->value)[2] == 'U') &&
                   ((entry->value)[3] == 'e' || (entry->value)[3] == 'E')) 
    return TRUE;
  
  return FALSE;
 }

void 
config_free(config_handle *handle)
 {
  config_entry *cur;
  config_entry *next;

  if (handle == NULL) return;

  if (handle->list != NULL)
   {
    for (cur = handle->list; cur != NULL; cur = next)
     {
      if (cur->key != NULL)   free(cur->key);
      if (cur->value != NULL) free(cur->value);
      if ((cur->n_values > 0) && (cur->values != NULL))
       {
        int i;
        for(i=0; i<cur->n_values; i++)
         {
          if (cur->values[i] != NULL) free(cur->values[i]);
         }
        free(cur->values);
       }
      next = cur->next;
      free(cur);
     }
   }

  if ((handle->config_path) != NULL) free(handle->config_path);
  if ((handle->install_path) != NULL) free(handle->install_path);
  if ((handle->bin_path) != NULL) free(handle->bin_path);

  free(handle);
 }

#ifdef CONFIG_TEST_CODE

#include <unistd.h>

#define TEST_CODE_PATH "/tmp/blah_config_test_XXXXXX"

int
main(int argc, char *argv[])
{
  int tcf;
  char *path;
  const char *test_config =
    "\n"
    "# This is a test configuration file \n"
    " #\n\n"
    "a=123\n"
    "  # Merrily merrily quirky \n"
    "#But parsable by /bin/sh \n"
    "b=b_value\n"
    " c=Junk  \n"
    " c=c_value   \n"
    " b1=tRuE \n"
    "b2=1 \n"
    "b3=Yes\n"
    "b4=0\n"
    "b4=\"   Junk\"\n"
    "b5=\" False\"\n"
    "file=/tmp/test_`whoami`.bjr\n"
    "arr[0]=value_0\n"
    "arr[3]=value_3\n"
    "\n";

  config_handle *cha;
  config_entry *ret;
  int r=0;

  path = (char *)malloc(strlen(TEST_CODE_PATH)+1);
  if (path == NULL)
   {
    fprintf(stderr,"%s: Out of memory.\n",argv[0]);
    return 1;
   }
  strcpy(path,TEST_CODE_PATH);

  tcf = mkstemp(path);

  if (tcf < 0)
   {
    fprintf(stderr,"%s: Error creating temporary file %s: ",argv[0],path);
    perror("");
    return 2;
   }

  if (write(tcf, test_config, strlen(test_config)) < strlen(test_config) )
   {
    fprintf(stderr,"%s: Error writing to temporary file %s: ",argv[0],path);
    perror("");
    return 3;
   }
  
  close(tcf);

  setenv("BLAHPD_CONFIG_LOCATION",path,1);
  cha = config_read(NULL);
  unlink(path);
  if (cha == NULL)
   {
    fprintf(stderr,"%s: Error reading config from %s: ",argv[0],path);
    return 4;
   }

  ret = config_get("a",cha);
  if (ret == NULL) fprintf(stderr,"%s: key a not found\n",argv[0]),r=5;
  else if (atoi(ret->value) != 123) fprintf(stderr,"%s: key a != 123\n",argv[0]),r=6;
  ret = config_get("b",cha);
  if (ret == NULL) fprintf(stderr,"%s: key b not found\n",argv[0]),r=7;
  else if (strcmp(ret->value, "b_value") != 0) fprintf(stderr,"%s: key b != b_value\n",argv[0]),r=8;
  ret = config_get("c",cha);
  if (ret == NULL) fprintf(stderr,"%s: key c not found\n",argv[0]),r=9;
  else if (strcmp(ret->value, "c_value") != 0) fprintf(stderr,"%s: key c != c_value\n",argv[0]),r=10;

  ret = config_get("b1",cha);
  if (ret == NULL) fprintf(stderr,"%s: key b1 not found\n",argv[0]),r=11;
  else if (!config_test_boolean(ret)) fprintf(stderr,"%s: key b1 is false\n",argv[0]),r=12;
  ret = config_get("b2",cha);
  if (ret == NULL) fprintf(stderr,"%s: key b2 not found\n",argv[0]),r=13;
  else if (!config_test_boolean(ret)) fprintf(stderr,"%s: key b2 is false\n",argv[0]),r=14;
  ret = config_get("b3",cha);
  if (ret == NULL) fprintf(stderr,"%s: key b3 not found\n",argv[0]),r=15;
  else if (!config_test_boolean(ret)) fprintf(stderr,"%s: key b3 is false\n",argv[0]),r=16;
  ret = config_get("b4",cha);
  if (ret == NULL) fprintf(stderr,"%s: key b4 not found\n",argv[0]),r=17;
  else if (config_test_boolean(ret)) fprintf(stderr,"%s: key b4 is true\n",argv[0]),r=18;
  ret = config_get("b5",cha);
  if (ret == NULL) fprintf(stderr,"%s: key b5 not found\n",argv[0]),r=18;
  else if (config_test_boolean(ret)) fprintf(stderr,"%s: key b5 is true\n",argv[0]),r=19;
  ret = config_get("file",cha);
  if (ret == NULL) fprintf(stderr,"%s: key file not found\n",argv[0]),r=19;
  printf("file == <%s>\n",ret->value);

  ret = config_get("arr",cha);
  if (ret == NULL) fprintf(stderr,"%s: key arr not found\n",argv[0]),r=20;
  if (ret->n_values != 2) fprintf(stderr,"%s: arr contains %d values instead of 2\n",argv[0],ret->n_values),r=21;
  else
   {
    printf("arr value 0 == <%s>\n",ret->values[0]);
    printf("arr value 1 == <%s>\n",ret->values[1]);
   }

  config_free(cha);

  return r;
}

#endif /*defined CONFIG_TEST_CODE*/
