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
	char *tpath;
	
	int version=0;
	int first=TRUE;
	int tmptim;
	int finstr_len=0;
	int loop_interval=DEFAULT_LOOP_INTERVAL;
	
	bact.njobs = 0;
	bact.jobs = NULL;
	
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
	}else{
		debug = 0;
	}
	
        ret = config_get("pbs_binpath",cha);
        if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key pbs_binpath not found\n",argv0);
        } else {
                pbs_binpath=strdup(ret->value);
                if(pbs_binpath == NULL){
                        sysfatal("strdup failed for pbs_binpath in main: %r");
                }
        }
	
        ret = config_get("pbs_spoolpath",cha);
        if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key pbs_spoolpath not found\n",argv0);
        } else {
                pbs_spoolpath=strdup(ret->value);
                if(pbs_spoolpath == NULL){
                        sysfatal("strdup failed for pbs_spoolpath in main: %r");
                }

		tpath=make_message("%s/server_logs",pbs_spoolpath);
		if (opendir(tpath)==NULL){
			do_log(debuglogfile, debug, 1, "%s: dir %s does not exist or is not readable\n",argv0,tpath);
			sysfatal("dir %s does not exist or is not readable: %r",tpath);
		}
		free(tpath);
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

	ret = config_get("bupdater_loop_interval",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bupdater_loop_interval not found using the default:%d\n",argv0,loop_interval);
	} else {
		loop_interval=atoi(ret->value);
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
			if(job_registry_purge(registry_file, now-purge_interval,0)<0){
				do_log(debuglogfile, debug, 1, "%s: Error purging job registry %s\n",argv0,registry_file);
                	        fprintf(stderr,"%s: Error purging job registry %s :",argv0,registry_file);
                	        perror("");

			}else{
				purge_time=time(0);
			}
		}
	       
		IntStateQuery();
		
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
	time_t tmstampepoch;
	char *batch_str=NULL;
	char *wn_str=NULL; 
        char *twn_str=NULL;
        char *status_str=NULL;
	char *cp=NULL;
	char *command_string=NULL;
	job_registry_entry *ren=NULL;
	int first=TRUE;

	command_string=make_message("%s/qstat -f",pbs_binpath);
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
			do_log(debuglogfile, debug, 4, "%s: line in IntStateQuery is:%s\n",argv0,line);
			if(line && strstr(line,"Job Id: ")){
				if(!first && en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren && ren->status==HELD) || (en.status==IDLE && en.updater_info && strcmp(en.updater_info,"found")==0)) && ren && (en.status!=ren->status)){
                        		if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum, JOB_REGISTRY_UPDATE_WN_ADDR|JOB_REGISTRY_UPDATE_STATUS|JOB_REGISTRY_UPDATE_UDATE|JOB_REGISTRY_UPDATE_UPDATER_INFO)) < 0){
						if(ret != JOB_REGISTRY_NOT_FOUND){
                	                		fprintf(stderr,"Update of record returns %d: ",ret);
							perror("");
						}
					} else {
						do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.wn_addr,en.status);
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
					JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,"found");
				}else if(status_str && strcmp(status_str,"W")==0){ 
					en.status=IDLE;
					JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,"found");
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
				JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,"found");
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
				timestamp=make_message("%s %s %s %s %s",token[2],token[3],token[4],token[5],token[6]);
                        	tmstampepoch=str2epoch(timestamp,"L");
				free(timestamp);
				en.udate=tmstampepoch;
				freetoken(&token,maxtok_t);
			}
			free(line);
		}
		pclose(fp);
	}
	
	if(en.status!=UNDEFINED && (en.status!=IDLE || (en.status==IDLE && ren && ren->status==HELD) || (en.status==IDLE && en.updater_info && strcmp(en.updater_info,"found")==0)) && ren && (en.status!=ren->status)){
		if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum, JOB_REGISTRY_UPDATE_WN_ADDR|JOB_REGISTRY_UPDATE_STATUS|JOB_REGISTRY_UPDATE_UDATE|JOB_REGISTRY_UPDATE_UPDATER_INFO)) < 0){
			if(ret != JOB_REGISTRY_NOT_FOUND){
				fprintf(stderr,"Update of record returns %d: ",ret);
				perror("");
			}
		} else {
			do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.wn_addr,en.status);
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
	time_t tmstampepoch;
	char *exit_str=NULL;
	int failed_count=0;
	int time_to_add=0;
	time_t now;
	char *cp=NULL;
	char *command_string=NULL;
	char *pbs_spool=NULL;

	do_log(debuglogfile, debug, 3, "%s: input_string in FinalStateQuery is:%s\n",argv0,input_string);
	
	maxtok_j = strtoken(input_string, ':', &jobid);
	
	for(k=0;k<maxtok_j;k++){
	
		if(jobid[k] && strlen(jobid[k])==0) continue;

		if(pbs_spoolpath){
			pbs_spool=make_message("-p %s",pbs_spoolpath);
			command_string=make_message("%s/tracejob %s -m -l -a %s",pbs_binpath,pbs_spool,jobid[k]);
		}else{
			command_string=make_message("%s/tracejob -m -l -a %s",pbs_binpath,jobid[k]);
		}
		
		fp = popen(command_string,"r");
		
		do_log(debuglogfile, debug, 3, "%s: command_string in FinalStateQuery is:%s\n",argv0,command_string);

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
                        	do_log(debuglogfile, debug, 3, "%s: line in FinalStateQuery is:%s\n",argv0,line);
				if(line && (strstr(line,"Job deleted") || (strstr(line,"dequeuing from") && strstr(line,"state RUNNING")))){	
					maxtok_t = strtoken(line, ' ', &token);
					timestamp=make_message("%s %s",token[0],token[1]);
					tmstampepoch=str2epoch(timestamp,"A");
					free(timestamp);
					freetoken(&token,maxtok_t);
					en.udate=tmstampepoch;
					en.status=REMOVED;
                        		en.exitcode=-999;
					JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
				}else if(line && strstr(line,"Exit_status=") && en.status != REMOVED){	
					maxtok_t = strtoken(line, ' ', &token);
					timestamp=make_message("%s %s",token[0],token[1]);
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
				do_log(debuglogfile, debug, 2, "%s: registry update in FinalStateQuery for: jobid=%s exitcode=%d status=%d\n",argv0,en.batch_id,en.exitcode,en.status);
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
	do_log(debuglogfile, debug, 3, "%s: next FinalStatequery will be in %d seconds\n",argv0,time_to_add);
	
	freetoken(&jobid,maxtok_j);
	return 0;
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
	printf("Usage: BUpdaterPBS [OPTION...]\n");
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
	printf("Usage: BUpdaterPBS [-ov?] [-o|--nodaemon] [-v|--version] [-?|--help] [--usage]\n");
	exit(EXIT_SUCCESS);
}
