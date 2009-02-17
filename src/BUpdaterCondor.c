/*
#  File:     BUpdaterCondor.c
#
#  Author:   Massimo Mezzadri
#  e-mail:   Massimo.Mezzadri@mi.infn.it
# 
# Copyright (c) Members of the EGEE Collaboration. 2004. 
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

#include "BUpdaterCondor.h"

extern int bfunctions_poll_timeout;

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
	int qlen;
	int first=TRUE;
        int tmptim;
	char *dgbtimestamp;
	int loop_interval=5;
	
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

	ret = config_get("bupdater_child_poll_timeout",cha);
	if (ret != NULL){
		tmptim=atoi(ret->value);
		if (tmptim > 0) bfunctions_poll_timeout = tmptim*1000;
	}

	ret = config_get("bupdater_debug_level",cha);
	if (ret != NULL){
		debug=atoi(ret->value);
	}
	
	ret = config_get("bupdater_debug_logfile",cha);
	if (ret != NULL){
		debuglogname=strdup(ret->value);
	}
	if(debug <=0){
		debug=0;
	}
    
	if(debuglogname){
		if((debuglogfile = fopen(debuglogname, "a+"))==0){
			debuglogfile =  fopen("/dev/null", "a+");
		}
	}
	
        ret = config_get("condor_binpath",cha);
        if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key condor_binpath not found\n",dgbtimestamp,argv0);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
        } else {
                condor_binpath=strdup(ret->value);
        }
	
	ret = config_get("job_registry",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key job_registry not found\n",dgbtimestamp,argv0);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		registry_file=strdup(ret->value);
	}
	
	ret = config_get("purge_interval",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key purge_interval not found using the default:%d\n",dgbtimestamp,argv0,purge_interval);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		purge_interval=atoi(ret->value);
	}
	
	ret = config_get("finalstate_query_interval",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key finalstate_query_interval not found using the default:%d\n",dgbtimestamp,argv0,finalstate_query_interval);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		finalstate_query_interval=atoi(ret->value);
	}
	
	ret = config_get("alldone_interval",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key alldone_interval not found using the default:%d\n",dgbtimestamp,argv0,alldone_interval);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		alldone_interval=atoi(ret->value);
	}
	
	ret = config_get("bupdater_loop_interval",cha);
	if (ret == NULL){
		if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key bupdater_loop_interval not found using the default:%d\n",dgbtimestamp,argv0,loop_interval);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		loop_interval=atoi(ret->value);
	}

	ret = config_get("bupdater_pidfile",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key bupdater_pidfile not found\n",dgbtimestamp,argv0);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		pidfile=strdup(ret->value);
	}
	
	if( !nodmn ) daemonize();


	if( pidfile ){
		writepid(pidfile);
		free(pidfile);
	}
	
	config_free(cha);

	for(;;){
		/* Purge old entries from registry */
		now=time(0);
		if(now - purge_time > 86400){
			if(job_registry_purge(registry_file, now-purge_interval,0)<0){

				if(debug){
					dgbtimestamp=iepoch2str(time(0));
					fprintf(debuglogfile, "%s %s: Error purging job registry %s\n",dgbtimestamp,argv0,registry_file);
					fflush(debuglogfile);
					free(dgbtimestamp);
				}
                        	fprintf(stderr,"%s: Error purging job registry %s :",argv0,registry_file);
                        	perror("");

			}else{
				purge_time=time(0);
			}
		}
	       
		rha=job_registry_init(registry_file, BY_BATCH_ID);
		if (rha == NULL)
		{
			if(debug){
				dgbtimestamp=iepoch2str(time(0));
				fprintf(debuglogfile, "%s %s: Error initialising job registry %s\n",dgbtimestamp,argv0,registry_file);
				fflush(debuglogfile);
				free(dgbtimestamp);
			}
			fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
			perror("");
			sleep(loop_interval);
			continue;
		}

		IntStateQuery();
		
		fd = job_registry_open(rha, "r");
		if (fd == NULL)
		{
			if(debug){
				dgbtimestamp=iepoch2str(time(0));
				fprintf(debuglogfile, "%s %s: Error opening job registry %s\n",dgbtimestamp,argv0,registry_file);
				fflush(debuglogfile);
				free(dgbtimestamp);
			}
			fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
			perror("");
			sleep(loop_interval);
			continue;
		}
		if (job_registry_rdlock(rha, fd) < 0)
		{
			if(debug){
				dgbtimestamp=iepoch2str(time(0));
				fprintf(debuglogfile, "%s %s: Error read locking job registry %s\n",dgbtimestamp,argv0,registry_file);
				fflush(debuglogfile);
				free(dgbtimestamp);
			}
			fprintf(stderr,"%s: Error read locking job registry %s :",argv0,registry_file);
			perror("");
			sleep(loop_interval);
			continue;
		}

		first=TRUE;
		
		while ((en = job_registry_get_next(rha, fd)) != NULL){

			if(en->status!=REMOVED && en->status!=COMPLETED){
				/* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4)*/
				if(now-en->mdate>alldone_interval){
					AssignFinalState(en->batch_id);	
					free(en);
					continue;
				}
			
				if(now-en->mdate>finalstate_query_interval){
					/* create the constraint that will be used in condor_history command in FinalStateQuery*/
					if(!first) strcat(query," ||");	
					if(first) first=FALSE;
					if((constraint=malloc(strlen(en->batch_id) + 14)) == 0){
						sysfatal("can't malloc constraint %r");
        				}
					sprintf(constraint," ClusterId==%s",en->batch_id);
					
					if (query != NULL) qlen = strlen(query);
					else               qlen = 0;
					q=realloc(query,qlen+strlen(constraint)+4);
					
					if(q != NULL){
						if (query != NULL) strcat(q,constraint);
						else               strcpy(q,constraint);
						query=q;	
					}else{
						sysfatal("can't realloc query: %r");
					}
					free(constraint);
					runfinal=TRUE;
				}
			}
			free(en);
		}
		
		if(runfinal){
			FinalStateQuery(query);
			runfinal=FALSE;
		}
		if (query != NULL){
			free(query);
			query = NULL;
		}
		fclose(fd);		
		job_registry_destroy(rha);
		sleep(loop_interval);
	}
	
	return 0;
	
}


int
IntStateQuery()
{
/*
 Output format for status query for unfinished jobs for condor:
 batch_id   user      status     executable     exitcode   udate(timestamp_for_current_status)
 22018     gliteuser  2          /bin/sleep     0          1202288920

 Filled entries:
 batch_id
 status
 exitcode
 udate
 
 Filled by suhmit script:
 blah_id 
 
 Unfilled entries:
 wn_addr
 exitreason
*/

        FILE *fp;
	int len;
	char *line;
	char **token;
	int maxtok_t=0;
	job_registry_entry en;
	int ret;
	char *cp; 
	char *dgbtimestamp;
	char *command_string;
	job_registry_entry *ren=NULL;

	if((command_string=malloc(strlen(condor_binpath) + NUM_CHARS)) == 0){
		sysfatal("can't malloc command_string %r");
	}
	
	sprintf(command_string,"%s/condor_q -format \"%%d \" ClusterId -format \"%%s \" Owner -format \"%%d \" JobStatus -format \"%%s \" Cmd -format \"%%s \" ExitStatus -format \"%%s\\n\" EnteredCurrentStatus|grep -v condorc-",condor_binpath);
	fp = popen(command_string,"r");

	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && (strlen(line)==0 || strncmp(line,"JOBID",5)==0)){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
	
			maxtok_t = strtoken(line, ' ', &token);
			if (maxtok_t < 6){
				freetoken(&token,maxtok_t);
				free(line);
				continue;
			}
		
			JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,token[0]);
			en.status=atoi(token[2]);
			en.exitcode=atoi(token[4]);
			en.udate=atoi(token[5]);
			JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
			JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
			
			if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
					fprintf(stderr,"Get of record returns error ");
					perror("");
			}
				
			if(en.status!=UNDEFINED && en.status!=IDLE && (en.status!=ren->status)){	
				if ((ret=job_registry_update(rha, &en)) < 0){
					if(ret != JOB_REGISTRY_NOT_FOUND){
						fprintf(stderr,"Append of record returns %d: ",ret);
						perror("");
					}
				}
				if(debug>1){
					dgbtimestamp=iepoch2str(time(0));
					fprintf(debuglogfile, "%s %s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
					fflush(debuglogfile);
					free(dgbtimestamp);
				}
			}
		
			freetoken(&token,maxtok_t);
			free(line);
			free(ren);
		}
		pclose(fp);
	}

	free(command_string);
	return 0;
}

int
FinalStateQuery(char *query)
{
/*
 Output format for status query for finished jobs for condor:
 batch_id   user      status     executable     exitcode   udate(timestamp_for_current_status)
 22018     gliteuser  4          /bin/sleep     0          1202288920

 Filled entries:
 batch_id
 status
 exitcode
 udate
 
 Filled by suhmit script:
 blah_id 
 
 Unfilled entries:
 wn_addr
 exitreason
*/
        FILE *fp;
	int len;
	char *line;
	char **token;
	int maxtok_t=0;
	job_registry_entry en;
	int ret;
	char *cp; 
	char *dgbtimestamp;
	char *command_string;

	if((command_string=malloc(NUM_CHARS + strlen(query) +strlen(condor_binpath))) == 0){
		sysfatal("can't malloc command_string %r");
	}

	sprintf(command_string,"%s/condor_history -constraint \"%s\" -format \"%%d \" ClusterId -format \"%%s \" Owner -format \"%%d \" JobStatus -format \"%%s \" Cmd -format \"%%s \" ExitStatus -format \"%%s\\n\" EnteredCurrentStatus",condor_binpath,query);
	fp = popen(command_string,"r");

	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && (strlen(line)==0 || strncmp(line,"JOBID",5)==0)){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
			
			maxtok_t = strtoken(line, ' ', &token);
			if (maxtok_t < 6){
				freetoken(&token,maxtok_t);
				free(line);
				continue;
			}
		
			JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,token[0]);
			en.status=atoi(token[2]);
			en.exitcode=atoi(token[4]);
			en.udate=atoi(token[5]);
                	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
                	JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
		
			if(en.status!=UNDEFINED && en.status!=IDLE){	
				if ((ret=job_registry_update(rha, &en)) < 0){
					if(ret != JOB_REGISTRY_NOT_FOUND){
						fprintf(stderr,"Append of record returns %d: ",ret);
						perror("");
					}
				}
		
				if(debug>1){
					dgbtimestamp=iepoch2str(time(0));
					fprintf(debuglogfile, "%s %s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
					fflush(debuglogfile);
					free(dgbtimestamp);
				}
			}
			freetoken(&token,maxtok_t);
			free(line);
		}
		pclose(fp);
	}

	free(command_string);
	return 0;
}

int AssignFinalState(char *batchid){

	job_registry_entry en;
	int ret,i;
	time_t now;
	char *dgbtimestamp;

	now=time(0);
	
	JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batchid);
	en.status=COMPLETED;
	en.exitcode=999;
	en.udate=now;
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
	JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
		
	if ((ret=job_registry_update(rha, &en)) < 0){
		if(ret != JOB_REGISTRY_NOT_FOUND){
			fprintf(stderr,"Append of record %d returns %d: ",i,ret);
			perror("");
		}
	}
	if(debug>1){
		dgbtimestamp=iepoch2str(time(0));
		fprintf(debuglogfile, "%s %s: registry update in AssignStateQuery for: jobid=%s creamjobid=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.status);
		fflush(debuglogfile);
		free(dgbtimestamp);
	}
	
	return 0;
}
