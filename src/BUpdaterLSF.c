/*
#  File:     BUpdaterLSF.c
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

#include "BUpdaterLSF.h"

extern int bfunctions_poll_timeout;

int main(int argc, char *argv[]){

	FILE *fd;
	job_registry_entry *en;
	time_t now;
	time_t purge_time=0;
	char *pidfile=NULL;
	
	poptContext poptcon;
	int rc=0;			     
	int version=0;
	int first=TRUE;
	int tmptim;
	char *dgbtimestamp;
	time_t finalquery_start_date;
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
	
        ret = config_get("lsf_binpath",cha);
        if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key lsf_binpath not found\n",dgbtimestamp,argv0);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
        } else {
                lsf_binpath=strdup(ret->value);
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
	
	ret = config_get("bhist_logs_to_read",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key bhist_logs_to_read not found using the default:%d\n",dgbtimestamp,argv0,bhist_logs_to_read);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		bhist_logs_to_read=atoi(ret->value);
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

	ret = config_get("bupdater_loop_interval",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key bupdater_loop_interval not found - using the default:%d\n",dgbtimestamp,argv0,loop_interval);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		loop_interval=atoi(ret->value);
	}
	
	ret = config_get("bupdater_bjobs_long_format",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key bupdater_bjobs_long_format not found - using the default:%d\n",dgbtimestamp,argv0,bjobs_long_format);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		bjobs_long_format=strdup(ret->value);
	}
	
	ret = config_get("bupdater_use_bhist_for_susp",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: key bupdater_use_bhist_for_susp not found - using the default:%d\n",dgbtimestamp,argv0,use_bhist_for_susp);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	} else {
		use_bhist_for_susp=strdup(ret->value);
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
			if((rc=job_registry_purge(registry_file, now-purge_interval,0))<0){

				if(debug){
					dgbtimestamp=iepoch2str(time(0));
					fprintf(debuglogfile, "%s %s: Error purging job registry %s:%d\n",dgbtimestamp,argv0,registry_file,rc);
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
		if (rha == NULL){
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
		if(bjobs_long_format && strcmp(bjobs_long_format,"yes")==0){ 
			IntStateQuery();
		}else{
			IntStateQueryShort();
		}
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

		first=TRUE;
		finalquery_start_date = time(0);
		
		while ((en = job_registry_get_next(rha, fd)) != NULL){

			if((bupdater_lookup_active_jobs(&bact,en->batch_id) != BUPDATER_ACTIVE_JOBS_SUCCESS) && en->status!=REMOVED && en->status!=COMPLETED){
				/* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4)*/
				if(now-en->mdate>alldone_interval){
					AssignFinalState(en->batch_id);
					free(en);
					continue;
				}
			
				if(now-en->mdate>finalstate_query_interval){
					if (en->mdate < finalquery_start_date) finalquery_start_date=en->mdate;
					runfinal=TRUE;
				}
			}
			free(en);
		}
		
		if(runfinal){
			FinalStateQuery(finalquery_start_date);
			runfinal=FALSE;
		}
		fclose(fd);		
		job_registry_destroy(rha);
		sleep(loop_interval);
	}
	
	return 0;
	
}


int
IntStateQueryShort()
{

/*
 Filled entries:
 batch_id
 wn_addr
 status
 exitcode
 udate
 
 Filled by submit script:
 blah_id 
 
 Unfilled entries:
 exitreason
*/


        FILE *fp;
	int len;
	char *line=NULL;
	char **token;
	int maxtok_l=0,i;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *dgbtimestamp;
	char *tmp=NULL; 
	char *cp=NULL; 
	char *command_string=NULL;
	job_registry_entry *ren=NULL;
	int isresumed=0;
	int first=1;

	if((command_string=malloc(strlen(lsf_binpath) + 17)) == 0){
		sysfatal("can't malloc command_string %r");
	}
	
	sprintf(command_string,"%s/bjobs -u all -w",lsf_binpath);
	fp = popen(command_string,"r");

	en.status=UNDEFINED;
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"N/A");
	bupdater_free_active_jobs(&bact);
	
	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && (strlen(line)==0 || strncmp(line,"JOBID",5)==0)){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
			tmp=strdup(line);
			if ((cp = strchr (tmp, ' ')) != NULL){
				*cp = '\0';
			}
			
			if(job_registry_get_recnum(rha,tmp)==0){
				free(line);
				free(tmp);
				continue;
			}
			if(!first && en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren->status==HELD)) && ren && (en.status!=ren->status)){	
				if ((ret=job_registry_update(rha, &en)) < 0){
					if(ret != JOB_REGISTRY_NOT_FOUND){
						fprintf(stderr,"Append of record returns %d: ",ret);
						perror("");
					}
				}
				if(debug>1){
					dgbtimestamp=iepoch2str(time(0));
					fprintf(debuglogfile, "%s %s: registry update in IntStateQueryShort for: jobid=%s creamjobid=%s wn=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
					fflush(debuglogfile);
					free(dgbtimestamp);
				}
				en.status = UNDEFINED;
			}
				
			JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,tmp);
			bupdater_push_active_job(&bact, en.batch_id);
			free(tmp);
			
			maxtok_l = strtoken(line, ' ', &token);
			
			JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,token[5]);
			
			if(!first) free(ren);
			if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
					fprintf(stderr,"Get of record returns error ");
					perror("");
			}
			first=0;        
			if(token[2] && strcmp(token[2],"PEND")==0){ 
				en.status=IDLE;
			}else if(token[2] && (strcmp(token[2],"USUSP")==0) || (strcmp(token[2],"PSUSP")==0) ||(strcmp(token[2],"SSUSP")==0)){ 
				en.status=HELD;
			}else if(token[2] && strcmp(token[2],"RUN")==0){ 
				en.status=RUNNING;
			}
			
			if((timestamp=malloc(strlen(token[7]) + strlen(token[8]) + strlen(token[9]) + 4)) == 0){
				sysfatal("can't malloc timestamp: %r");
			}
			sprintf(timestamp,"%s %s %s",token[7],token[8],token[9]);
			tmstampepoch=str2epoch(timestamp,"V");
			free(timestamp);
			en.udate=tmstampepoch;
		
			freetoken(&token,maxtok_l);
			
			free(line);
		}
		pclose(fp);
	}
	
	if(en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren->status==HELD)) && ren && (en.status!=ren->status)){	
		if ((ret=job_registry_update(rha, &en)) < 0){
			if(ret != JOB_REGISTRY_NOT_FOUND){
				fprintf(stderr,"Append of record returns %d: ",ret);
				perror("");
			}
		}
		if(debug>1){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: registry update in IntStateQueryShort for: jobid=%s creamjobid=%s wn=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	}				

	free(ren);
	free(command_string);
	return 0;
}

int
IntStateQuery()
{

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
	int len;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *dgbtimestamp;
	char *tmp=NULL; 
	char *cp=NULL; 
	char *wn_str=NULL; 
	char *batch_str=NULL;
	char *command_string=NULL;
	job_registry_entry *ren=NULL;
	int isresumed=0;
	int first=1;

	if((command_string=malloc(strlen(lsf_binpath) + 17)) == 0){
		sysfatal("can't malloc command_string %r");
	}
	
	sprintf(command_string,"%s/bjobs -u all -l",lsf_binpath);
	fp = popen(command_string,"r");

	en.status=UNDEFINED;
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
			if(debug>2){
				dgbtimestamp=iepoch2str(time(0));
				fprintf(debuglogfile, "%s %s: line in IntStateQuery is:%s\n",dgbtimestamp,argv0,line);
				fflush(debuglogfile);
				free(dgbtimestamp);
			}	
			if(line && strstr(line,"Job <")){
				isresumed=0;
				if(!first && en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren->status==HELD)) && ren && (en.status!=ren->status)){	
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
					en.status = UNDEFINED;
				}
				maxtok_t = strtoken(line, ',', &token);
				batch_str=strdel(token[0],"Job <>");
				JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_str);
				free(batch_str);
				freetoken(&token,maxtok_t);
				if(!first) free(ren);
				if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
						fprintf(stderr,"Get of record returns error ");
						perror("");
				}
				first=0;
			}else if(line && strstr(line," <PEND>, ")){	
				en.status=IDLE;
			}else if(line && strstr(line," <RUN>, ")){	
				en.status=RUNNING;
				if(use_bhist_for_susp && strcmp(use_bhist_for_susp,"yes")==0){
				/*If status was HELD we have to check timestamp of resume with bhist (the info is not there with bjobs)*/
					if(ren && ren->status==HELD){
						tmstampepoch=get_resume_timestamp(en.batch_id);
						en.udate=tmstampepoch;
						isresumed=1;
					}
				}
			}else if(line && (strstr(line," <USUSP>,") || strstr(line," <PSUSP>,") || strstr(line," <SSUSP>,"))){	
				en.status=HELD;
				/*If status is HELD we check timestamp of suspension with bhist (the info is not there with bjobs)*/
				if(use_bhist_for_susp && strcmp(use_bhist_for_susp,"yes")==0){ 
					tmstampepoch=get_susp_timestamp(en.batch_id);
					en.udate=tmstampepoch;
				}
			}else if(line && strstr(line,"Started on ") && (en.status == RUNNING) && (isresumed == 0)){	
				maxtok_t = strtoken(line, ' ', &token);
                        	if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + strlen(token[2]) + strlen(token[3]) + 4)) == 0){
					sysfatal("can't malloc timestamp in IntStateQuery: %r");
				}
				sprintf(timestamp,"%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				en.udate=tmstampepoch;
				en.status=RUNNING;
				free(timestamp);
				wn_str=strdel(token[6],"<>,");
				JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,wn_str);
				free(wn_str);
				freetoken(&token,maxtok_t);
			}
			free(line);
		}
		pclose(fp);
	}
		
	if(en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren->status==HELD)) && ren && (en.status!=ren->status)){	
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

	free(ren);
	free(command_string);
	return 0;
}

int
FinalStateQuery(time_t start_date)
{
/*
bhist -u all -a -l
In line:

Tue Mar 18 13:47:32: Exited with exit code 2. The CPU time used is 1.8 seconds;
or
Tue Mar 18 12:48:24: Done successfully. The CPU time used is 2.1 seconds;

there are:
udate for the final state (Tue Mar 18 13:47:32):
exitcode (=0 if Done successfully) or (from Exited with exit code 2)

*/

/*
 Filled entries:
 batch_id
 status
 exitcode
 wn_addr
 udate
 
 Filled by submit script:
 blah_id 
 
 Unfilled entries:
 exitreason
*/

        FILE *fp;
	int len;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *batch_str=NULL;
	char *ex_str=NULL; 
	char *cp=NULL; 
	char *dgbtimestamp;
	struct tm start_date_tm;
	char start_date_str[80];
	char *command_string=NULL;

	if((command_string=malloc(strlen(lsf_binpath) + NUM_CHARS + sizeof(start_date_str) + 24)) == 0){
		sysfatal("can't malloc command_string %r");
	}

	sprintf(command_string,"%s/bhist -u all -d -l -n %d",lsf_binpath,bhist_logs_to_read);
	if (localtime_r(&start_date, &start_date_tm) != NULL){
		if (strftime(start_date_str, sizeof(start_date_str), " -C %Y/%m/%d/%H:%M,", &start_date_tm) > 0){
			strcat(command_string,start_date_str);
		}
	}

	fp = popen(command_string,"r");
	
	if(debug>2){
		dgbtimestamp=iepoch2str(time(0));
		fprintf(debuglogfile, "%s %s: command_string in FinalStateQuery is:%s\n",dgbtimestamp,argv0,command_string);
		fflush(debuglogfile);
		free(dgbtimestamp);
	}

	en.status=UNDEFINED;
	JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
	
	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && strlen(line)==0){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
			if(debug>2){
				dgbtimestamp=iepoch2str(time(0));
				fprintf(debuglogfile, "%s %s: line in FinalStateQuery is:%s\n",dgbtimestamp,argv0,line);
				fflush(debuglogfile);
				free(dgbtimestamp);
			}	
			if(line && strstr(line,"Job <")){	
				if(en.status!=UNDEFINED && en.status!=IDLE){	
					if ((ret=job_registry_update_select(rha, &en,
					JOB_REGISTRY_UPDATE_UDATE |
					JOB_REGISTRY_UPDATE_STATUS |
					JOB_REGISTRY_UPDATE_EXITCODE |
					JOB_REGISTRY_UPDATE_EXITREASON )) < 0){
						if(ret != JOB_REGISTRY_NOT_FOUND){
                	                		fprintf(stderr,"Append of record returns %d: ",ret);
							perror("");
						}
					}
					if(debug>1){
						dgbtimestamp=iepoch2str(time(0));
						fprintf(debuglogfile, "%s %s: registry update in FinalStateQuery for: jobid=%s creamjobid=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.status);
						fflush(debuglogfile);
						free(dgbtimestamp);
					}
					en.status = UNDEFINED;
				}				
				maxtok_t = strtoken(line, ',', &token);
				batch_str=strdel(token[0],"Job <>");
				JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_str);
				free(batch_str);
				freetoken(&token,maxtok_t);
			}else if(line && strstr(line," Signal <KILL>")){	
				maxtok_t = strtoken(line, ' ', &token);
                        	if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + strlen(token[2]) + strlen(token[3]) + 4)) == 0){
					sysfatal("can't malloc timestamp in FinalStateQuery: %r");
				}
				sprintf(timestamp,"%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				en.udate=tmstampepoch;
				en.status=REMOVED;
				free(timestamp);
				en.exitcode=-999;
				JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
				freetoken(&token,maxtok_t);
			}else if(line && strstr(line," Exited with exit code") && en.status != REMOVED){	
				maxtok_t = strtoken(line, ' ', &token);
                        	if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + strlen(token[2]) + strlen(token[3]) + 4)) == 0){
					sysfatal("can't malloc timestamp in FinalStateQuery: %r");
				}
				sprintf(timestamp,"%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				en.udate=tmstampepoch;
				en.status=COMPLETED;
				free(timestamp);
				ex_str=strdel(token[8],".");
				en.exitcode=atoi(ex_str);
				free(ex_str);
				JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
				freetoken(&token,maxtok_t);
			}else if(line && strstr(line," Done successfully") && en.status != REMOVED){	
				maxtok_t = strtoken(line, ' ', &token);
                        	if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + strlen(token[2]) + strlen(token[3]) + 4)) == 0){
					sysfatal("can't malloc timestamp in FinalStateQuery: %r");
				}
				sprintf(timestamp,"%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				en.udate=tmstampepoch;
				en.status=COMPLETED;
				free(timestamp);
				en.exitcode=0;
				JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
				freetoken(&token,maxtok_t);
			}
			free(line);
		}
		pclose(fp);
	}
	
	if(en.status!=UNDEFINED && en.status!=IDLE){	
		if ((ret=job_registry_update_select(rha, &en,
		JOB_REGISTRY_UPDATE_UDATE |
		JOB_REGISTRY_UPDATE_STATUS |
		JOB_REGISTRY_UPDATE_EXITCODE |
		JOB_REGISTRY_UPDATE_EXITREASON )) < 0){
			if(ret != JOB_REGISTRY_NOT_FOUND){
				fprintf(stderr,"Append of record returns %d: ",ret);
				perror("");
			}
		}
		if(debug>1){
			dgbtimestamp=iepoch2str(time(0));
			fprintf(debuglogfile, "%s %s: f registry update in FinalStateQuery for: jobid=%s creamjobid=%s status=%d\n",dgbtimestamp,argv0,en.batch_id,en.user_prefix,en.status);
			fflush(debuglogfile);
			free(dgbtimestamp);
		}
	}				

	free(command_string);
	return 0;
}

int
get_susp_timestamp(char *jobid)
{

        FILE *fp;
	int len;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *cp=NULL; 
	char *dgbtimestamp;
	char *command_string=NULL;
	
	if((command_string=malloc(strlen(lsf_binpath) + NUM_CHARS + 20)) == 0){
		sysfatal("can't malloc command_string %r");
	}

	sprintf(command_string,"%s/bhist -u all -l %s",lsf_binpath,jobid);

	fp = popen(command_string,"r");
		
	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && strlen(line)==0){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
			if(line && strstr(line," Suspended by")){	
				maxtok_t = strtoken(line, ' ', &token);
                        	if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + strlen(token[2]) + strlen(token[3]) + 4)) == 0){
					sysfatal("can't malloc timestamp in get_susp_timestamp: %r");
				}
				sprintf(timestamp,"%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				free(timestamp);
				freetoken(&token,maxtok_t);
			}
			free(line);
		}
		pclose(fp);
	}
	

	free(command_string);
	return tmstampepoch;
}

int
get_resume_timestamp(char *jobid)
{

        FILE *fp;
	int len;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *cp=NULL; 
	char *dgbtimestamp;
	char *command_string=NULL;
	
	if((command_string=malloc(strlen(lsf_binpath) + NUM_CHARS + 20)) == 0){
		sysfatal("can't malloc command_string %r");
	}

	sprintf(command_string,"%s/bhist -u all -l %s",lsf_binpath,jobid);

	fp = popen(command_string,"r");
		
	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && strlen(line)==0){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
			if(line && strstr(line," Running;")){	
				maxtok_t = strtoken(line, ' ', &token);
                        	if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + strlen(token[2]) + strlen(token[3]) + 4)) == 0){
					sysfatal("can't malloc timestamp in get_susp_timestamp: %r");
				}
				sprintf(timestamp,"%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				free(timestamp);
				freetoken(&token,maxtok_t);
			}
			free(line);
		}
		pclose(fp);
	}
	

	free(command_string);
	return tmstampepoch;
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
