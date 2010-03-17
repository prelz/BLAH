/*
  File: condor_create_log.c
  Author : Giuseppe Fiorentino
  Email: giuseppe.fiorentino@mi.infn.it

  Description:
  Exported function int create_accounting_log(int argc, char** argname, char** argvalue)
  for accounting functionality 

  Copyright (c) Members of the EGEE Collaboration. 2007-2010. 

    See http://www.eu-egee.org/partners/ for details on the copyright
    holders.  
  
    Licensed under the Apache License, Version 2.0 (the "License"); 
    you may not use this file except in compliance with the License. 
    You may obtain a copy of the License at 
  
        http://www.apache.org/licenses/LICENSE-2.0 
  
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
    See the License for the specific language governing permissions and 
    limitations under the License.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/stat.h>
#include "condor_create_log.h"
#include "config.h"
#include "blah_utils.h"

#define MAX_TEMP_ARRAY_SIZE              1000
#define DEFAULT_GLITE_LOCATION  	"/opt/glite"

static char* acc_loc=NULL;
static config_handle *blah_config_handle=NULL;

/* prototypes */
int logAccInfo(char* jobId, char* gridjobid, char* ce_id, char* queue, char* fqan, char* userDN);
int getProxyInfo(char* proxname, char* fqan, char* userDN);
char* lower(char* tlowstring);
char* getValue(char* field, int fieldnum, char** name, char** value);

int create_accounting_log(int argc, char** argname, char** argvalue)
{
	char* proxyname=NULL;
	char* gridjobid=NULL;
	char* jobId=NULL;
	char* ceid=NULL;
        char* queue=NULL;
        char fqan[MAX_TEMP_ARRAY_SIZE], userDN[MAX_TEMP_ARRAY_SIZE]; 
        char* acc_needed_libs=NULL;
        char* acc_old_ld_lib=NULL;
        char* acc_new_ld_lib=NULL;

	/* Mandatory fields */
        if(!(proxyname=getValue("x509userproxy",argc,argname,argvalue))) return 1;
        if(!(jobId=getValue("jobid",argc,argname,argvalue))) return 1;
        /* Optional fields */ 
        gridjobid=getValue("edg_jobid",argc,argname,argvalue);
        ceid=getValue("ceid",argc,argname,argvalue);
        queue=getValue("queue",argc,argname,argvalue);

        /* Get values from environment */
        if ((acc_loc = getenv("GLITE_LOCATION")) == NULL)
        {
                acc_loc = DEFAULT_GLITE_LOCATION;
        }

        acc_needed_libs = make_message("%s/lib:%s/externals/lib:%s/lib:/opt/lcg/lib", acc_loc, acc_loc,
			                getenv("GLOBUS_LOCATION") ? getenv("GLOBUS_LOCATION") : "/opt/globus");
        acc_old_ld_lib=getenv("LD_LIBRARY_PATH");
        if(acc_old_ld_lib)
        {
                acc_new_ld_lib =(char*)malloc(strlen(acc_old_ld_lib) + 2 + strlen(acc_needed_libs));
                sprintf(acc_new_ld_lib,"%s;%s",acc_old_ld_lib,acc_needed_libs);
                setenv("LD_LIBRARY_PATH",acc_new_ld_lib,1);
        }else
                 setenv("LD_LIBRARY_PATH",acc_needed_libs,1);

        blah_config_handle = config_read(NULL);
        if (blah_config_handle == NULL) 
		return 1;
        if(getProxyInfo(proxyname, fqan, userDN)) 
		return 1;
        if(userDN) 
		logAccInfo(jobId, gridjobid, ceid, queue, fqan, userDN);
	return 0;
}

int  logAccInfo(char* jobId, char* gridjobid, char* ce_id, char* queue, char* fqan, char* userDN)
{
        int i=0, rc=0, cs=0, result=0, fd = -1, count = 0, slen = 0, slen2 = 0;
        FILE *cmd_out=NULL;
        FILE *conf_file=NULL;
        char *log_line;
        char *ce_idtmp=NULL;
	char *jobid=NULL;
        char *temp_str=NULL;
        char date_str[MAX_TEMP_ARRAY_SIZE];
        time_t tt;
        struct tm *t_m=NULL;
        char host_name[MAX_TEMP_ARRAY_SIZE];
        int id;
        char bs[4];
        char *esc_userDN=NULL;
        char uid[MAX_TEMP_ARRAY_SIZE];

        /* Submission time */
        time(&tt);
        t_m = gmtime(&tt);
        sprintf(date_str,"%04d-%02d-%02d\\ %02d:%02d:%02d", 1900+t_m->tm_year, t_m->tm_mon+1, t_m->tm_mday, t_m->tm_hour, t_m->tm_min, t_m->tm_sec);

        /* These data must be logged in the log file:
         "timestamp=<submission time to LRMS>" "userDN=<user's DN>" "userFQAN=<user's FQAN>"
         "ceID=<CE ID>" "jobID=<grid job ID>" "lrmsID=<LRMS job ID>"
        */
        /* grid jobID  : if we are here we suppose that the edg_jobid is present, if not we log an empty string*/

        if(!gridjobid) gridjobid=make_message("");


	/* ce_id and queue are passed as parameters*/
        if(!ce_id)
        {
                gethostname(host_name, MAX_TEMP_ARRAY_SIZE);
                if(queue)
                {
                        ce_id=make_message("%s:2119/blah-%s-%s",host_name,jobId,queue);
                }else
                        ce_id=make_message("%s:2119/blah-%s-",host_name,jobId);
        }else
        {
                if(queue&&(strncmp(&ce_id[strlen(ce_id) - strlen(queue)],queue,strlen(queue))))
                {
                        ce_idtmp=make_message("%s-%s",ce_id,queue);
                        ce_id=ce_idtmp;
                }
        }
        
        /*UID*/
	sprintf(uid,"%d",getuid());
        /* log line with in addiction unixuser */
        // call to suided tool
        esc_userDN=escape_spaces(userDN);
        log_line=make_message("%s/BDlogger %s \\\"timestamp=%s\\\"\\ \\\"userDN=%s\\\"\\ %s\\\"ceID=%s\\\"\\ \\\"jobID=%s\\\"\\ \\\"lrmsID=%s\\\"\\ \\\"localUser=%s\\\"", blah_config_handle->bin_path, blah_config_handle->config_path, date_str, esc_userDN, fqan, ce_id, gridjobid, jobId, uid);
        system(log_line);

        if(gridjobid) free(gridjobid);
        free(log_line);
        free(esc_userDN);
        if (!strcmp(ce_id," ")) free(ce_id);
        memset(fqan,0,MAX_TEMP_ARRAY_SIZE);
        return 0;
}

int getProxyInfo(char* proxname, char* fqan, char* userDN)
{
        /* command : openssl x509 -in proxname -subject -noout */
        char temp_str[MAX_TEMP_ARRAY_SIZE];
        FILE *cmd_out=NULL;
        int  result=0;
        int  slen=0;
        int  count=0;
        char fqanlong[MAX_TEMP_ARRAY_SIZE];
        sprintf(temp_str,"openssl x509 -in %s  -subject -noout", proxname);
        if ((cmd_out=popen(temp_str, "r")) == NULL)
                return 1;
        fgets(fqanlong, MAX_TEMP_ARRAY_SIZE, cmd_out);
        result = pclose(cmd_out);
        /* example:
           subject= /C=IT/O=INFN/OU=Personal Certificate/L=Milano/CN=Francesco
                    Prelz/Email=francesco.prelz@mi.infn.it/CN=proxy
                    CN=proxy, CN=limited must be elimnated from the bottom of the string
        */
         slen = strlen(fqanlong);
         while(1)
         {
                if (!strncmp(&fqanlong[slen - 10],"/CN=proxy",9))
                {
                      memset(&fqanlong[slen - 10],0,9);
                      slen -=9;
                }else
                if (!strncmp(&fqanlong[slen - 18],"/CN=limited proxy",17))
                {
                      memset(&fqanlong[slen - 18],0,17);
                      slen -=17;
                }else
                          break;
          }
          strcpy(userDN,&fqanlong[9]);
          slen-=9;
          if(userDN[slen - 1] == '/') userDN[slen - 1] = 0;
          /* user'sFQAN detection */
          fqanlong[0]=0;
          memset(fqanlong,0,MAX_TEMP_ARRAY_SIZE);
          /* command : voms-proxy-info -file proxname -fqan  */
          memset(temp_str,0,MAX_TEMP_ARRAY_SIZE);
          sprintf(temp_str,"%s/voms-proxy-info -file %s -fqan 2> /dev/null", blah_config_handle->bin_path, proxname);
          if ((cmd_out=popen(temp_str, "r")) == NULL)
                return 1;
          while(fgets(fqanlong, MAX_TEMP_ARRAY_SIZE, cmd_out))
          {
                strcat(fqan,"\\\"userFQAN=");
                strcat(fqan,fqanlong);
                memset(fqanlong,0,MAX_TEMP_ARRAY_SIZE);
                if(fqan[strlen(fqan)-1]=='\n') fqan[strlen(fqan)-1] = 0;
                strcat(fqan,"\\\"\\ ");
          }
         if (!strcmp(fqan,"")) sprintf(fqan,"\\\"userFQAN=\\\"\\ ");
         result = pclose(cmd_out);
         return 0;
}

/* Utility functions */

char* getValue(char* field, int fieldnum, char** name, char** value)
{
	int i=0;
	while (i<fieldnum)
	{
		if(!strcmp(lower(field),lower(name[i])))
		return value[i];
		i++;
	}
	return NULL;
}

char* lower(char* tlowstring)
{
  	int i = 0;
	char *lowstr=strdup(tlowstring);
	while(tlowstring[i] !=0 )
	{
        	lowstr[i] = tolower(tlowstring[i]);
		i++;
	}
	lowstr[i]=0;
	return lowstr;
}

