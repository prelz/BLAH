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
	
	int version=0;
	int first=TRUE;
	int tmptim;
	time_t finalquery_start_date;
	int loop_interval=DEFAULT_LOOP_INTERVAL;
	
	bact.njobs = 0;
	bact.jobs = NULL;
	
	int rc;				
	int c;				

        static int help;
        static int short_help;
	
	while (1) {
		static struct option long_options[] =
		{
		{"help",      no_argument,     &help,       1},
		{"usage",     no_argument,     &short_help, 1},
		{"nodaemon",  no_argument,       0, 'o'},
		{"version",   no_argument,       0, 'v'},
		{0, 0, 0, 0}
		};

		int option_index = 0;
     
		c = getopt_long (argc, argv, "vo",long_options, &option_index);
     
		if (c == -1){
			break;
		}
     
		switch (c)
		{

		case 0:
		if (long_options[option_index].flag != 0){
			break;
		}
     
		case 'v':
			version=1;
			break;
	       
		case 'o':
			nodmn=1;
			break;

		case '?':
			break;
     
		default:
			abort ();
		}
	}
	
	if(help){
		usage();
	}
	 
	if(short_help){
		short_usage();
	}
	
	argv0 = argv[0];
	
        signal(SIGHUP,sighup);

	if(version) {
		printf("%s Version: %s\n",progname,VERSION);
		exit(EXIT_SUCCESS);
	}   

        /* Checking configuration */
        check_config_file("UPDATER");

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
	} else {
		debug = 0;
	}
	
        ret = config_get("lsf_binpath",cha);
        if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key lsf_binpath not found\n",argv0);
        } else {
                lsf_binpath=strdup(ret->value);
                if(lsf_binpath == NULL){
                        sysfatal("strdup failed for lsf_binpath in main: %r");
                }
        }
	
	ret = config_get("job_registry",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key job_registry not found\n",argv0);
		sysfatal("job_registry not defined. Exiting");
	} else {
		registry_file=strdup(ret->value);
                if(registry_file == NULL){
                        sysfatal("strdup failed for registry_file in main: %r");
                }
	}
	
	ret = config_get("purge_interval",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key purge_interval not found using the default:%d\n",argv0,purge_interval);
	} else {
		purge_interval=atoi(ret->value);
	}
	
	ret = config_get("finalstate_query_interval",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key finalstate_query_interval not found using the default:%d\n",argv0,finalstate_query_interval);
	} else {
		finalstate_query_interval=atoi(ret->value);
	}
	
	ret = config_get("alldone_interval",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key alldone_interval not found using the default:%d\n",argv0,alldone_interval);
	} else {
		alldone_interval=atoi(ret->value);
	}
	
	ret = config_get("bhist_logs_to_read",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bhist_logs_to_read not found using the default:%d\n",argv0,bhist_logs_to_read);
	} else {
		bhist_logs_to_read=atoi(ret->value);
	}
	
	ret = config_get("bupdater_pidfile",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bupdater_pidfile not found\n",argv0);
	} else {
		pidfile=strdup(ret->value);
                if(pidfile == NULL){
                        sysfatal("strdup failed for pidfile in main: %r");
                }
	}

	ret = config_get("bupdater_loop_interval",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bupdater_loop_interval not found - using the default:%d\n",argv0,loop_interval);
	} else {
		loop_interval=atoi(ret->value);
	}
	
	ret = config_get("bupdater_bjobs_long_format",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bupdater_bjobs_long_format not found - using the default:%s\n",argv0,bjobs_long_format);
	} else {
		bjobs_long_format=strdup(ret->value);
                if(bjobs_long_format == NULL){
                        sysfatal("strdup failed for bjobs_long_format in main: %r");
                }
	}
	
	ret = config_get("bupdater_use_bhist_for_susp",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bupdater_use_bhist_for_susp not found - using the default:%s\n",argv0,use_bhist_for_susp);
	} else {
		use_bhist_for_susp=strdup(ret->value);
                if(use_bhist_for_susp == NULL){
                        sysfatal("strdup failed for use_bhist_for_susp in main: %r");
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
		do_log(debuglogfile, debug, 1, "%s: Error initialising job registry %s\n",argv0,registry_file);
		fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
		perror("");
	}

	for(;;){
		/* Purge old entries from registry */
		now=time(0);
		if(now - purge_time > 86400){
			if((rc=job_registry_purge(registry_file, now-purge_interval,0))<0){
				do_log(debuglogfile, debug, 1, "%s: Error purging job registry %s:%d\n",argv0,registry_file,rc);
                	        fprintf(stderr,"%s: Error purging job registry %s :",argv0,registry_file);
                	        perror("");

			}else{
				purge_time=time(0);
			}
		}
	       
		if(bjobs_long_format && strcmp(bjobs_long_format,"yes")==0){ 
			IntStateQuery();
		}else{
			IntStateQueryShort();
		}
		
		fd = job_registry_open(rha, "r");
		if (fd == NULL){
			do_log(debuglogfile, debug, 1, "%s: Error opening job registry %s\n",argv0,registry_file);
			fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
			perror("");
			sleep(loop_interval);
			continue;
		}
		if (job_registry_rdlock(rha, fd) < 0){
			do_log(debuglogfile, debug, 1, "%s: Error read locking job registry %s\n",argv0,registry_file);
			fprintf(stderr,"%s: Error read locking job registry %s :",argv0,registry_file);
			perror("");
			sleep(loop_interval);
			continue;
		}
		job_registry_firstrec(rha,fd);
		fseek(fd,0L,SEEK_SET);
		
		first=TRUE;
		finalquery_start_date = time(0);
		
		while ((en = job_registry_get_next(rha, fd)) != NULL){

			if((bupdater_lookup_active_jobs(&bact,en->batch_id) != BUPDATER_ACTIVE_JOBS_SUCCESS) && en->status!=REMOVED && en->status!=COMPLETED){

				if(now-en->mdate>finalstate_query_interval){
					if (en->mdate < finalquery_start_date) finalquery_start_date=en->mdate;
					runfinal=TRUE;
					free(en);
					continue;
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
			FinalStateQuery(finalquery_start_date);
			runfinal=FALSE;
		}
		fclose(fd);		
		sleep(loop_interval);
	}
	
	job_registry_destroy(rha);
	
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
 udate
 
 Filled by submit script:
 blah_id 
 
 Unfilled entries:
 exitreason
 exitcode
*/


        FILE *fp;
	char *line=NULL;
	char **token;
	int maxtok_l=0;
	job_registry_entry en;
	int ret;
	char *timestamp;
	time_t tmstampepoch;
	char *tmp=NULL; 
	char *cp=NULL; 
	char *command_string=NULL;
	job_registry_entry *ren=NULL;
	int first=TRUE;

	command_string=make_message("%s/bjobs -u all -w",lsf_binpath);
	fp = popen(command_string,"r");

	en.status=UNDEFINED;
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
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
                	if(tmp == NULL){
                        	sysfatal("strdup failed for tmp in IntStateQueryShort: %r");
                	}
			if ((cp = strchr (tmp, ' ')) != NULL){
				*cp = '\0';
			}
			
			if(job_registry_get_recnum(rha,tmp)==0){
				free(line);
				free(tmp);
				continue;
			}
			if(!first && en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren && ren->status==HELD)) && ren && (en.status!=ren->status)){	
				if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum, JOB_REGISTRY_UPDATE_WN_ADDR|JOB_REGISTRY_UPDATE_STATUS|JOB_REGISTRY_UPDATE_UDATE)) < 0){
					if(ret != JOB_REGISTRY_NOT_FOUND){
						fprintf(stderr,"Update of record returns %d: ",ret);
						perror("");
					}
				} else {
					do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQueryShort for: jobid=%s creamjobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
					if (en.status == REMOVED || en.status == COMPLETED)
						job_registry_unlink_proxy(rha, &en);
				}
				en.status = UNDEFINED;
				JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
			}
				
			JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,tmp);
			bupdater_push_active_job(&bact, en.batch_id);
			free(tmp);
			
			maxtok_l = strtoken(line, ' ', &token);
			
			JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,token[5]);
			
			if(!first) free(ren);
			if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
					fprintf(stderr,"Get of record returns error for %s ",en.batch_id);
					perror("");
			}
			first=FALSE;        
			if(token[2] && strcmp(token[2],"PEND")==0){ 
				en.status=IDLE;
			}else if(token[2] && ((strcmp(token[2],"USUSP")==0) || (strcmp(token[2],"PSUSP")==0) ||(strcmp(token[2],"SSUSP")==0))){ 
				en.status=HELD;
			}else if(token[2] && strcmp(token[2],"RUN")==0){ 
				en.status=RUNNING;
			}
			
			timestamp=make_message("%s %s %s",token[7],token[8],token[9]);
			tmstampepoch=str2epoch(timestamp,"V");
			free(timestamp);
			en.udate=tmstampepoch;
		
			freetoken(&token,maxtok_l);
			
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
			do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQueryShort for: jobid=%s creamjobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
			if (en.status == REMOVED || en.status == COMPLETED)
				job_registry_unlink_proxy(rha, &en);
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
 exitcode
 exitreason
*/


        FILE *fp;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	job_registry_entry en;
	int ret;
	char *timestamp;
	time_t tmstampepoch;
	char *cp=NULL; 
	char *wn_str=NULL; 
	char *batch_str=NULL;
	char *command_string=NULL;
	job_registry_entry *ren=NULL;
	int isresumed=FALSE;
	int first=TRUE;

	command_string=make_message("%s/bjobs -u all -l",lsf_binpath);
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
			do_log(debuglogfile, debug, 3, "%s: line in IntStateQuery is:%s\n",argv0,line);
			if(line && strstr(line,"Job <")){
				isresumed=FALSE;
				if(!first && en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren && ren->status==HELD)) && ren && (en.status!=ren->status)){	
					if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum, JOB_REGISTRY_UPDATE_WN_ADDR|JOB_REGISTRY_UPDATE_STATUS|JOB_REGISTRY_UPDATE_UDATE)) < 0){
						if(ret != JOB_REGISTRY_NOT_FOUND){
							fprintf(stderr,"Update of record returns %d: ",ret);
							perror("");
						}
					} else {
						do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
						if (en.status == REMOVED || en.status == COMPLETED)
							job_registry_unlink_proxy(rha, &en);
					}
					en.status = UNDEFINED;
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
				}
				maxtok_t = strtoken(line, ',', &token);
				batch_str=strdel(token[0],"Job <>");
				JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_str);
				bupdater_push_active_job(&bact, en.batch_id);
				free(batch_str);
				freetoken(&token,maxtok_t);
				if(!first) free(ren);
				if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
						fprintf(stderr,"Get of record returns error ");
						perror("");
				}
				first=FALSE;
			}else if(line && strstr(line," <PEND>, ")){	
				en.status=IDLE;
				if(use_bhist_for_susp && strcmp(use_bhist_for_susp,"yes")==0){
				/*If status was HELD we have to check timestamp of resume to pend with bhist (the info is not there with bjobs)*/
					if(ren && ren->status==HELD){
						tmstampepoch=get_pend_timestamp(en.batch_id);
						en.udate=tmstampepoch;
					}
				}
				if(ren && (ren->status==IDLE || ren->status==HELD)){
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
				}
			}else if(line && strstr(line," <RUN>, ")){	
				en.status=RUNNING;
				if(use_bhist_for_susp && strcmp(use_bhist_for_susp,"yes")==0){
				/*If status was HELD we have to check timestamp of resume with bhist (the info is not there with bjobs)*/
					if(ren && ren->status==HELD){
						tmstampepoch=get_resume_timestamp(en.batch_id);
						en.udate=tmstampepoch;
						isresumed=TRUE;
					}
				}
			}else if(line && (strstr(line," <USUSP>,") || strstr(line," <PSUSP>,") || strstr(line," <SSUSP>,"))){	
				en.status=HELD;
				if(ren && ren->status==IDLE){
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
				}
				/*If status is HELD we check timestamp of suspension with bhist (the info is not there with bjobs)*/
				if(use_bhist_for_susp && strcmp(use_bhist_for_susp,"yes")==0){ 
					tmstampepoch=get_susp_timestamp(en.batch_id);
					en.udate=tmstampepoch;
				}
			}else if(line && strstr(line,"Started on ") && (en.status == RUNNING) && (!isresumed)){	
				maxtok_t = strtoken(line, ' ', &token);
				timestamp=make_message("%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				en.udate=tmstampepoch;
				en.status=RUNNING;
				free(timestamp);
				wn_str=strdel(token[6],"<>,;");
				JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,wn_str);
				free(wn_str);
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
			do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
			if (en.status == REMOVED || en.status == COMPLETED)
				job_registry_unlink_proxy(rha, &en);
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
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	job_registry_entry en;
	int ret;
	char *timestamp;
	time_t tmstampepoch;
	char *batch_str=NULL;
	char *ex_str=NULL; 
	char *cp=NULL; 
	struct tm start_date_tm;
	char start_date_str[80];
	char *command_string=NULL;

	localtime_r(&start_date, &start_date_tm);
	strftime(start_date_str, sizeof(start_date_str), "%Y/%m/%d/%H:%M,", &start_date_tm);
	command_string=make_message("%s/bhist -u all -d -l -n %d -C %s",lsf_binpath,bhist_logs_to_read,start_date_str);

	fp = popen(command_string,"r");
	
	do_log(debuglogfile, debug, 3, "%s: command_string in FinalStateQuery is:%s\n",argv0,command_string);

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
			do_log(debuglogfile, debug, 3, "%s: line in FinalStateQuery is:%s\n",argv0,line);
			if(line && strstr(line,"Job <")){	
				if(en.status!=UNDEFINED && en.status!=IDLE){	
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
						do_log(debuglogfile, debug, 2, "%s: registry update in FinalStateQuery for: jobid=%s creamjobid=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.status);
						if (en.status == REMOVED || en.status == COMPLETED)
							job_registry_unlink_proxy(rha, &en);
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
				timestamp=make_message("%s %s %s %s",token[0],token[1],token[2],token[3]);
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
				timestamp=make_message("%s %s %s %s",token[0],token[1],token[2],token[3]);
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
				timestamp=make_message("%s %s %s %s",token[0],token[1],token[2],token[3]);
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
				fprintf(stderr,"Update of record returns %d: ",ret);
				perror("");
			}
		} else {
			do_log(debuglogfile, debug, 2, "%s: f registry update in FinalStateQuery for: jobid=%s creamjobid=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.status);
			if (en.status == REMOVED || en.status == COMPLETED)
				job_registry_unlink_proxy(rha, &en);
		}
	}				

	free(command_string);
	return 0;
}

time_t
get_susp_timestamp(char *jobid)
{

        FILE *fp;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	char *timestamp;
	time_t tmstampepoch;
	char *cp=NULL; 
	char *command_string=NULL;
	
	command_string=make_message("%s/bhist -u all -l %s",lsf_binpath,jobid);

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
				timestamp=make_message("%s %s %s %s",token[0],token[1],token[2],token[3]);
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

time_t
get_resume_timestamp(char *jobid)
{

        FILE *fp;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	char *timestamp;
	time_t tmstampepoch;
	char *cp=NULL; 
	char *command_string=NULL;
	
	command_string=make_message("%s/bhist -u all -l %s",lsf_binpath,jobid);

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
				timestamp=make_message("%s %s %s %s",token[0],token[1],token[2],token[3]);
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

time_t
get_pend_timestamp(char *jobid)
{

        FILE *fp;
	char *line=NULL;
	char **token;
	int maxtok_t=0;
	char *timestamp;
	time_t tmstampepoch;
	char *cp=NULL; 
	char *command_string=NULL;
	
	command_string=make_message("%s/bhist -u all -l %s",lsf_binpath,jobid);

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
			if(line && strstr(line," Pending: Waiting for scheduling after resumed")){	
				maxtok_t = strtoken(line, ' ', &token);
				timestamp=make_message("%s %s %s %s",token[0],token[1],token[2],token[3]);
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
		do_log(debuglogfile, debug, 2, "%s: registry update in AssignStateQuery for: jobid=%s creamjobid=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.status);
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

int 
usage()
{
	printf("Usage: BUpdaterLSF [OPTION...]\n");
	printf("  -o, --nodaemon     do not run as daemon\n");
	printf("  -v, --version      print version and exit\n");
	printf("\n");
	printf("Help options:\n");
	printf("  -?, --help         Show this help message\n");
	printf("  --usage            Display brief usage message\n");
	exit(EXIT_SUCCESS);
}

int 
short_usage()
{
	printf("Usage: BUpdaterLSF [-ov?] [-o|--nodaemon] [-v|--version] [-?|--help] [--usage]\n");
	exit(EXIT_SUCCESS);
}

