/*
#  File:     BUpdaterPBS.c
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

#include "BUpdaterPBS.h"

extern int bfunctions_poll_timeout;

int main(int argc, char *argv[]){

	FILE *fd;
	job_registry_entry *en;
	time_t now;
	time_t purge_time=0;
	char *pidfile=NULL;
	char *final_string=NULL;
	char *cp=NULL;
	
	poptContext poptcon;
	int rc=0;			     
	int version=0;
	int first=TRUE;
	int tmptim;
	char *dgbtimestamp;
	int finstr_len=0;
	int loop_interval=DEFAULT_LOOP_INTERVAL;
	
	bact.njobs = 0;
	bact.jobs = NULL;
	
	struct poptOption poptopt[] = {     
		{ "nodaemon",      'o', POPT_ARG_NONE,   &nodmn, 	    0, "do not run as daemon",    NULL },
		{ "version",       'v', POPT_ARG_NONE,   &version,	    0, "print version and exit",  NULL },
		POPT_AUTOHELP
		POPT_TABLEEND
	};
	
	argv0 = argv[0];

        signal(SIGHUP,sighup);

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
	
        ret = config_get("pbs_binpath",cha);
        if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key pbs_binpath not found\n",dgbtimestamp,argv0);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
        } else {
                pbs_binpath=strdup(ret->value);
                if(pbs_binpath == NULL){
                        sysfatal("strdup failed for pbs_binpath in main: %r");
                }
        }
	
        ret = config_get("pbs_spoolpath",cha);
        if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key pbs_spoolpath not found\n",dgbtimestamp,argv0);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
        } else {
                pbs_spoolpath=strdup(ret->value);
                if(pbs_spoolpath == NULL){
                        sysfatal("strdup failed for pbs_spoolpath in main: %r");
                }
        }
	
	ret = config_get("job_registry",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key job_registry not found\n",dgbtimestamp,argv0);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
		sysfatal("job_registry not defined. Exiting");
	} else {
		registry_file=strdup(ret->value);
                if(registry_file == NULL){
                        sysfatal("strdup failed for registry_file in main: %r");
                }
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
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: Error initialising job registry %s\n",dgbtimestamp,argv0,registry_file);
			fflush(debuglogfile);
			free(dgbtimestamp);
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
	       
		IntStateQuery();
		
		fd = job_registry_open(rha, "r");
		
		if (fd == NULL){
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
		if (job_registry_rdlock(rha, fd) < 0){
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
		job_registry_firstrec(rha,fd);
		fseek(fd,0L,SEEK_SET);
		
		first=TRUE;
		
		while ((en = job_registry_get_next(rha, fd)) != NULL){
			if((bupdater_lookup_active_jobs(&bact, en->batch_id) != BUPDATER_ACTIVE_JOBS_SUCCESS) && en->status!=REMOVED && en->status!=COMPLETED){
				
				if((now-en->mdate>finalstate_query_interval) && (now > next_finalstatequery)){
					if((final_string=realloc(final_string,finstr_len + strlen(en->batch_id) + 2)) == 0){
                       	        		sysfatal("can't malloc final_string: %r");
					} else {
						if (finstr_len == 0) final_string[0] = '\000';
					}
 					strcat(final_string,en->batch_id);
					strcat(final_string,":");
					finstr_len=strlen(final_string);
					runfinal=TRUE;
				}
				
				/* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4)*/
				if(now-en->mdate>alldone_interval && !runfinal){
					AssignFinalState(en->batch_id);	
					free(en);
					continue;
				}
			
			}
			free(en);
		}
		
		if(runfinal){
			if (final_string[finstr_len-1] == ':' && (cp = strrchr (final_string, ':')) != NULL){
				*cp = '\0';
			}
			FinalStateQuery(final_string);
			runfinal=FALSE;
		}
		if (final_string != NULL){
			free(final_string);		
			final_string = NULL;
			finstr_len = 0;
		}
		fclose(fd);		
		sleep(loop_interval);
	}
	
	job_registry_destroy(rha);
		
	return 0;
	
}


int
IntStateQuery()
{
/*
qstat -f

Job Id: 11.cream-12.pd.infn.it
    Job_Name = cream_579184706
    job_state = R
    ctime = Wed Apr 23 11:39:55 2008
    exec_host = cream-wn-029.pn.pd.infn.it/0
*/

/*
 Filled entries:
 batch_id
 wn_addr
 status
 udate
 
 Filled by submit script:
 blah_id 
 
 Unfilled entries:
 exitreason
*/


        FILE *fp;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *batch_str=NULL;
	char *wn_str=NULL; 
        char *twn_str=NULL;
        char *status_str=NULL;
	char *dgbtimestamp;
	char *cp=NULL;
	char *command_string=NULL;
	job_registry_entry *ren=NULL;
	int first=TRUE;

	if((command_string=malloc(strlen(pbs_binpath) + 10)) == 0){
		sysfatal("can't malloc command_string %r");
	}
		
	sprintf(command_string,"%s/qstat -f",pbs_binpath);
	fp = popen(command_string,"r");

	en.status=UNDEFINED;
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
	bupdater_free_active_jobs(&bact);

	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && strlen(line)==0){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
			if(debug>3){
				dgbtimestamp=iepoch2str(time(0));
				fprintf(debuglogfile, "%s %s: line in IntStateQuery is:%s\n",dgbtimestamp,argv0,line);
				fflush(debuglogfile);
				free(dgbtimestamp);
			}
			if(line && strstr(line,"Job Id: ")){
				if(!first && en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren && ren->status==HELD)) && ren && (en.status!=ren->status)){
                        		if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum, JOB_REGISTRY_UPDATE_WN_ADDR|JOB_REGISTRY_UPDATE_STATUS|JOB_REGISTRY_UPDATE_UDATE)) < 0){
						if(ret != JOB_REGISTRY_NOT_FOUND){
                	                		fprintf(stderr,"Update of record returns %d: ",ret);
							perror("");
						}
					} else {
						if(debug>1){
							dgbtimestamp=iepoch2str(time(0));
							fprintf(debuglogfile, "%s %s: registry update in IntStateQuery for: jobid=%s wn=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.wn_addr,en.status);
							fflush(debuglogfile);
							free(dgbtimestamp);
						}
						if (en.status == REMOVED || en.status == COMPLETED)
							job_registry_unlink_proxy(rha, &en);
					}
					en.status = UNDEFINED;
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
				}				
                        	maxtok_t = strtoken(line, ':', &token);
				batch_str=strdel(token[1]," ");
				JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_str);
				bupdater_push_active_job(&bact, en.batch_id);
				free(batch_str);
				freetoken(&token,maxtok_t);
				if(!first) free(ren);
				if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
						fprintf(stderr,"Get of record returns error for %s ",en.batch_id);
						perror("");
				}
				first=FALSE;				
			}else if(line && strstr(line,"job_state = ")){	
				maxtok_t = strtoken(line, '=', &token);
				status_str=strdel(token[1]," ");
				if(status_str && strcmp(status_str,"Q")==0){ 
					en.status=IDLE;
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
				}else if(status_str && strcmp(status_str,"W")==0){ 
					en.status=IDLE;
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
				}else if(status_str && strcmp(status_str,"R")==0){ 
					en.status=RUNNING;
				}else if(status_str && strcmp(status_str,"H")==0){ 
					en.status=HELD;
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
				}
				free(status_str);
				freetoken(&token,maxtok_t);
			}else if(line && strstr(line,"unable to run job")){
				en.status=IDLE;	
			}else if(line && strstr(line,"exec_host = ")){	
				maxtok_t = strtoken(line, '=', &token);
				twn_str=strdup(token[1]);
                		if(twn_str == NULL){
                        		sysfatal("strdup failed for twn_str in IntStateQuery: %r");
                		}
				freetoken(&token,maxtok_t);
				maxtok_t = strtoken(twn_str, '/', &token);
				wn_str=strdel(token[0]," ");
				JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,wn_str);
				free(twn_str);
 				free(wn_str);
				freetoken(&token,maxtok_t);
			}else if(line && strstr(line,"mtime = ")){	
                        	maxtok_t = strtoken(line, ' ', &token);
                        	if((timestamp=malloc(strlen(token[2]) + strlen(token[3]) + strlen(token[4]) + strlen(token[5]) + strlen(token[6]) + 6)) == 0){
                        	        sysfatal("can't malloc timestamp in IntStateQuery: %r");
                        	}
                        	sprintf(timestamp,"%s %s %s %s %s",token[2],token[3],token[4],token[5],token[6]);
                        	tmstampepoch=str2epoch(timestamp,"L");
				free(timestamp);
				en.udate=tmstampepoch;
				freetoken(&token,maxtok_t);
			}
			free(line);
		}
		pclose(fp);
	}
	
	if(en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren && ren->status==HELD)) && ren && (en.status!=ren->status)){
		if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum, JOB_REGISTRY_UPDATE_WN_ADDR|JOB_REGISTRY_UPDATE_STATUS|JOB_REGISTRY_UPDATE_UDATE)) < 0){
			if(ret != JOB_REGISTRY_NOT_FOUND){
				fprintf(stderr,"Update of record returns %d: ",ret);
				perror("");
			}
		} else {
			if(debug>1){
				dgbtimestamp=iepoch2str(time(0));
				fprintf(debuglogfile, "%s %s: registry update in IntStateQuery for: jobid=%s wn=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.wn_addr,en.status);
				fflush(debuglogfile);
				free(dgbtimestamp);
			}
			if (en.status == REMOVED || en.status == COMPLETED)
				job_registry_unlink_proxy(rha, &en);
		}
	}				

	free(ren);
	free(command_string);
	return 0;
}

int
FinalStateQuery(char *input_string)
{
/*
tracejob -m -l -a <jobid>
In line:

04/23/2008 11:50:43  S    Exit_status=0 resources_used.cput=00:00:01 resources_used.mem=11372kb resources_used.vmem=52804kb
                          resources_used.walltime=00:10:15

there are:
udate for the final state (04/23/2008 11:50:43):
exitcode Exit_status=

*/

/*
 Filled entries:
 batch_id (a list of jobid is given, one for each tracejob call)
 status (always a final state 3 or 4)
 exitcode
 udate
 
 Filled by submit script:
 blah_id 
 
 Unfilled entries:
 exitreason
*/
/*
[root@cream-12 server_logs]# tracejob -m -l -a 13

Job: 13.cream-12.pd.infn.it

04/23/2008 11:40:27  S    enqueuing into cream_1, state 1 hop 1
04/23/2008 11:40:27  S    Job Queued at request of infngrid002@cream-12.pd.infn.it, owner = infngrid002@cream-12.pd.infn.it, job name =
                          cream_365713239, queue = cream_1
04/23/2008 11:40:28  S    Job Modified at request of root@cream-12.pd.infn.it
04/23/2008 11:40:28  S    Job Run at request of root@cream-12.pd.infn.it
04/23/2008 11:50:43  S    Exit_status=0 resources_used.cput=00:00:01 resources_used.mem=11372kb resources_used.vmem=52804kb
                          resources_used.walltime=00:10:15
04/23/2008 11:50:44  S    dequeuing from cream_1, state COMPLETE
*/

        FILE *fp;
	char *line=NULL;
	char **token;
	char **jobid;
	int maxtok_t=0,maxtok_j=0,k;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *exit_str=NULL;
	int failed_count=0;
	int time_to_add=0;
	time_t now;
	char *dgbtimestamp;
	char *cp=NULL;
	char *command_string=NULL;
	char *pbs_spool=NULL;

	if(debug>2){
		dgbtimestamp=iepoch2str(time(0));
		fprintf(debuglogfile, "%s %s: input_string in FinalStateQuery is:%s\n",dgbtimestamp,argv0,input_string);
		fflush(debuglogfile);
		free(dgbtimestamp);
	}
	
	maxtok_j = strtoken(input_string, ':', &jobid);
	
	for(k=0;k<maxtok_j;k++){
	
		if(jobid[k] && strlen(jobid[k])==0) continue;

		if(pbs_spoolpath){
			if((pbs_spool=malloc(strlen(pbs_spoolpath) + 4)) == 0){
				sysfatal("can't malloc pbs_spool %r");
			}
			sprintf(pbs_spool,"-p %s",pbs_spoolpath);
			
			if((command_string=malloc(strlen(pbs_binpath) + strlen(pbs_spool) + strlen(jobid[k]) + 20)) == 0){
				sysfatal("can't malloc command_string %r");
			}
		
			sprintf(command_string,"%s/tracejob %s -m -l -a %s",pbs_binpath,pbs_spool,jobid[k]);
		}else{
			if((command_string=malloc(strlen(pbs_binpath) + strlen(jobid[k]) + 20)) == 0){
				sysfatal("can't malloc command_string %r");
			}
		
			sprintf(command_string,"%s/tracejob -m -l -a %s",pbs_binpath,jobid[k]);
		}
		
		fp = popen(command_string,"r");
		
		if(debug>2){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: command_string in FinalStateQuery is:%s\n",dgbtimestamp,argv0,command_string);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}

		/* en.status is set =0 (UNDEFINED) here and it is tested if it is !=0 before the registry update: the update is done only if en.status is !=0*/
		en.status=UNDEFINED;
		
		JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,jobid[k]);

		if(fp!=NULL){
			while(!feof(fp) && (line=get_line(fp))){
				if(line && strlen(line)==0){
					free(line);
					continue;
				}
				if ((cp = strrchr (line, '\n')) != NULL){
					*cp = '\0';
				}
				if(line && (strstr(line,"Job deleted") || (strstr(line,"dequeuing from") && strstr(line,"state RUNNING")))){	
					maxtok_t = strtoken(line, ' ', &token);
 					if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + 4)) == 0){
                        		        sysfatal("can't malloc timestamp in FinalStateQuery: %r");
                        		}
                        		sprintf(timestamp,"%s %s",token[0],token[1]);
					tmstampepoch=str2epoch(timestamp,"A");
					free(timestamp);
					freetoken(&token,maxtok_t);
					en.udate=tmstampepoch;
					en.status=REMOVED;
                        		en.exitcode=-999;
					JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
				}else if(line && strstr(line,"Exit_status=") && en.status != REMOVED){	
					maxtok_t = strtoken(line, ' ', &token);
 					if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + 4)) == 0){
                        		        sysfatal("can't malloc timestamp in FinalStateQuery: %r");
                        		}
                        		sprintf(timestamp,"%s %s",token[0],token[1]);
					tmstampepoch=str2epoch(timestamp,"A");
					exit_str=strdup(token[3]);
                			if(exit_str == NULL){
                        			sysfatal("strdup failed for exit_str in FinalStateQuery: %r");
                			}
					free(timestamp);
					freetoken(&token,maxtok_t);
					maxtok_t = strtoken(exit_str, '=', &token);
					free(exit_str);
					en.udate=tmstampepoch;
                        		en.exitcode=atoi(token[1]);
					en.status=COMPLETED;
					JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
					freetoken(&token,maxtok_t);
				}
				free(line);
			}
			pclose(fp);
		}
		
		if(en.status !=UNDEFINED && en.status!=IDLE){
			if ((ret=job_registry_update_select(rha, &en,
			JOB_REGISTRY_UPDATE_UDATE |
			JOB_REGISTRY_UPDATE_STATUS |
			JOB_REGISTRY_UPDATE_EXITCODE |
			JOB_REGISTRY_UPDATE_EXITREASON )) < 0){
				if(ret != JOB_REGISTRY_NOT_FOUND){
					fprintf(stderr,"Update of record returns %d: ",ret);
					perror("");
				}
			} else {
				if(debug>1){
					dgbtimestamp=iepoch2str(time(0));
					fprintf(debuglogfile, "%s %s: registry update in FinalStateQuery for: jobid=%s exitcode=%d status=%d\n",dgbtimestamp,argv0,en.batch_id,en.exitcode,en.status);
					fflush(debuglogfile);
					free(dgbtimestamp);
				}
				if (en.status == REMOVED || en.status == COMPLETED)
					job_registry_unlink_proxy(rha, &en);
			}
		}else{
			failed_count++;
		}		
		free(command_string);
		if(pbs_spoolpath) free(pbs_spool);
	}
	
	now=time(0);
	time_to_add=pow(failed_count,1.5);
	next_finalstatequery=now+time_to_add;
	if(debug>2){
		dgbtimestamp=iepoch2str(time(0));
		fprintf(debuglogfile, "%s %s: next FinalStatequery will be in %d seconds\n",dgbtimestamp,argv0,time_to_add);
		fflush(debuglogfile);
		free(dgbtimestamp);
	}
	
	freetoken(&jobid,maxtok_j);
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
			fprintf(stderr,"Update of record %d returns %d: ",i,ret);
			perror("");
		}
	} else {
		if(debug>1){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: registry update in AssignStateQuery for: jobid=%s creamjobid=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.status);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
		job_registry_unlink_proxy(rha, &en);
	}

	return 0;
}

void sighup()
{
        if(debug){
                fclose(debuglogfile);
                if((debuglogfile = fopen(debuglogname, "a+"))==0){
                        debug = 0;
                }
        }
}

