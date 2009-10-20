/*
# Copyright (c) Members of the EGEE Collaboration. 2009. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
# 
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
# 
#     http://www.apache.org/licenses/LICENSE-2.0 
# 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.
# 
*/

#include "BUpdaterSGE.h"

int main(int argc, char *argv[]){

	FILE *fd;
	job_registry_entry *en;
	time_t now;
	time_t purge_time=0;
	char *constraint=NULL;
	char *query=NULL;
	char *q=NULL;
	char *pidfile=NULL;
	
	poptContext poptcon;
	int rc=0;			     
	int version=0;
	int first=TRUE;
	
	struct poptOption poptopt[] = {     
		{ "nodaemon",      'o', POPT_ARG_NONE,   &nodmn, 	    0, "do not run as daemon",    NULL },
		{ "version",       'v', POPT_ARG_NONE,   &version,	    0, "print version and exit",  NULL },
		POPT_AUTOHELP
		POPT_TABLEEND
	};
		
	argv0 = argv[0];

	poptcon = poptGetContext(NULL, argc, (const char **) argv, poptopt, 0);
 
	if((rc = poptGetNextOpt(poptcon)) != -1){
		sysfatal("Invalid flag supplied: %r");
	}
	
	poptFreeContext(poptcon);
	
	if(version) {
		printf("%s Version: %s\n",progname,VERSION);
		exit(EXIT_SUCCESS);
	}   

	cha = config_read(NULL);
	if (cha == NULL)
	{
		fprintf(stderr,"Error reading config: ");
		perror("");
		return -1;
	}

	ret = config_get("debug_level",cha);
	if (ret != NULL){
		debug=atoi(ret->value);
	}
	
	ret = config_get("debug_logfile",cha);
	if (ret != NULL){
		debuglogname=strdup(ret->value);
                if(debuglogname == NULL){
                        sysfatal("strdup failed for debuglogname in main: %r");
                }
	}
	if(debug <=0){
		debug=0;
	}
    
	if(debuglogname){
		if((debuglogfile = fopen(debuglogname, "a+"))==0){
			debug = 0;
		}
	}else{
		debug = 0;
	}
	
        ret = config_get("sge_helper_path",cha);
        if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key sge_helper_path not found\n",argv0);
			fflush(debuglogfile);
		}
        } else {
                sge_helper_path=strdup(ret->value);
                if(sge_helper_path == NULL){
                        sysfatal("strdup failed for sge_helper_path in main: %r");
                }
        }
	
        ret = config_get("sge_root",cha);
        if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key sge_root not found\n",argv0);
			fflush(debuglogfile);
		}
        } else {
                sge_root=strdup(ret->value);
                if(sge_root == NULL){
                        sysfatal("strdup failed for sge_root in main: %r");
                }
        }
	
        ret = config_get("sge_cell",cha);
        if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key sge_cell not found\n",argv0);
			fflush(debuglogfile);
		}
        } else {
                sge_cell=strdup(ret->value);
                if(sge_cell == NULL){
                        sysfatal("strdup failed for sge_cell in main: %r");
                }
        }
	
	ret = config_get("job_registry",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key job_registry not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		registry_file=strdup(ret->value);
                if(registry_file == NULL){
                        sysfatal("strdup failed for registry_file in main: %r");
                }
	}
	
	ret = config_get("purge_interval",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key purge_interval not found using the default:%s\n",argv0,purge_interval);
			fflush(debuglogfile);
		}
	} else {
		purge_interval=atoi(ret->value);
	}
	
	ret = config_get("finalstate_query_interval",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key finalstate_query_interval not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		finalstate_query_interval=atoi(ret->value);
	}
	
	ret = config_get("alldone_interval",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key alldone_interval not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		alldone_interval=atoi(ret->value);
	}
	
	ret = config_get("bupdater_pidfile",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key bupdater_pidfile not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		pidfile=strdup(ret->value);
                if(pidfile == NULL){
                        sysfatal("strdup failed for pidfile in main: %r");
                }
	}
	
	if( !nodmn ) daemonize();


	if( pidfile ){
		writepid(pidfile);
		free(pidfile);
	}
	
	config_free(cha);

	rha=job_registry_init(registry_file, BY_BATCH_ID);
	
	if (rha == NULL){
		if(debug){
			fprintf(debuglogfile, "%s: Error initialising job registry %s\n",argv0,registry_file);
			fflush(debuglogfile);
		}
		fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
		perror("");
	}
		
	for(;;){
		/* Purge old entries from registry */
		now=time(0);
		if(now - purge_time > 86400){
			if(job_registry_purge(registry_file, now-purge_interval,0)<0){

				if(debug){
					fprintf(debuglogfile, "%s: Error purging job registry %s\n",argv0,registry_file);
					fflush(debuglogfile);
				}
                        	fprintf(stderr,"%s: Error purging job registry %s :",argv0,registry_file);
                        	perror("");
				sleep(2);

			}else{
				purge_time=time(0);
			}
		}

		IntStateQuery();
		
		fd = job_registry_open(rha, "r");
		if (fd == NULL)
		{
			if(debug){
				fprintf(debuglogfile, "%s: Error opening job registry %s\n",argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
			perror("");
			sleep(2);
			continue;
		}
		if (job_registry_rdlock(rha, fd) < 0)
		{
			if(debug){
				fprintf(debuglogfile, "%s: Error read locking job registry %s\n",argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error read locking job registry %s :",argv0,registry_file);
			perror("");
			sleep(2);
			continue;
		}

		if((constraint=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc constraint %r");
        	}
		if((query=calloc(CSTR_CHARS,1)) == 0){
			sysfatal("can't malloc query %r");
        	}
		query[0]='\0';
		first=TRUE;
		
		while ((en = job_registry_get_next(rha, fd)) != NULL)
		{

			if((now- en->mdate > finalstate_query_interval) && en->status!=3 && en->status!=4)
			{
				/* create the constraint that will be used in condor_history command in FinalStateQuery*/
				sprintf(constraint," %s",en->batch_id);
				
				q=realloc(query,strlen(query)+strlen(constraint)+1);
				if(q != NULL){
					query=q;	
				}else{
					if(debug){
						fprintf(debuglogfile, "can't realloc query\n");
						fflush(debuglogfile);
					}
                			fprintf(stderr,"%s: can't realloc query: ",argv[0]);
					perror("");
					sleep(2);
					continue;			
				}
				strcat(query,constraint);
				runfinal=TRUE;
			}
			
			/* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4) */
			if((now - en->mdate > alldone_interval) && en->status!=3 && en->status!=4 && !runfinal)
			{
				AssignFinalState(en->batch_id);	
			}
			
			free(en);
		}
		
		if(runfinal){
			FinalStateQuery(query);
			runfinal=FALSE;
		}
		free(constraint);		
		free(query);
		fclose(fd);		
		sleep(10); /* Was 2 seconds,  but that is a bit too frequent for running qstat etc */
	}
	
	job_registry_destroy(rha);
		
	return(0);
	
}


int
IntStateQuery()
{

        char *command_string;

	if((command_string=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc command_string %r");
	}
	
	sprintf(command_string,"%s --qstat --sgeroot=%s --cell=%s --all", sge_helper_path, sge_root, sge_cell);
	
	StateQuery( command_string );

	free(command_string);
	return(0);
}

int
FinalStateQuery(char *query)
{
        char *command_string;

	if((command_string=calloc(NUM_CHARS+strlen(query),1)) == 0){
		sysfatal("can't malloc command_string %r");
	}

	sprintf(command_string,"%s --sgeroot=%s --cell=%s --qacct %s", sge_helper_path, sge_root, sge_cell, query);

	StateQuery( command_string );

	free(command_string);
	return 0;
}



int StateQuery(char *command_string)
{
/*
 sge_helper output for unfinished jobs:
 batch_id  status  exitcode   udate(timestamp_for_current_status) workernode
 22018     2       0          1202288920			  hostname

 Filled entries:
 batch_id
 status
 exitcode
 udate
 wn_addr
 
 Filled by suhmit script:
 blah_id 
 
 Unfilled entries:
 exitreason
*/
	char *output;
        FILE *file_output;
	int len;
	char line[STR_CHARS];
	char *token[6];
	job_registry_entry en;
	int ret;

	file_output = popen(command_string,"r");

        if (file_output == NULL){
	  return 0;
	}

	while ( fgets( line, sizeof( line ), file_output ) > 0 ) {
   	        int i;
	        token[0] = strtok( line, " \n" );
		for ( i = 1 ; i <= 5 && token[i-1] != NULL ; i++ ) {
		  token[i] = strtok( NULL, " \n" );
		}

		if ( token[5] && strcmp( token[5], "OK" ) == 0 ) {
		  JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,token[0]);
		  en.status=atoi(token[1]);
		  en.exitcode=atoi(token[2]);
		  en.udate=atoi(token[3]);
		  JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,token[4]);
		  JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
		  
		  if ((ret=job_registry_update(rha, &en)) < 0)
		    {
		      fprintf(stderr,"Append of record returns %d: ", ret);
		      perror("");
		    }
		}
	}
	pclose( file_output );

	return(0);
}

int AssignFinalState(char *batchid){

	job_registry_entry en;
	int ret,i;
	time_t now;

	now=time(0);
	
	JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batchid);
	en.status=4;
	en.exitcode=-1;
	en.udate=now;
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
	JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
		
	if ((ret=job_registry_update(rha, &en)) < 0)
	{
		fprintf(stderr,"Append of record %d returns %d: ",i,ret);
		perror("");
	}
	return(0);
}
