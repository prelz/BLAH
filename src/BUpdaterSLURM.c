/*
#  File:     BUpdaterSLURM.c
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

#include "BUpdaterSLURM.h"

int main(int argc, char *argv[]){

	FILE *fd;
	job_registry_entry *en;
	time_t now;
	time_t purge_time=0;
	time_t last_consistency_check=0;
	char *pidfile=NULL;
	char *first_duplicate=NULL;
	
	struct pollfd *remupd_pollset = NULL;
	int remupd_nfds;
	
	int version=0;
	int first=TRUE;
	int tmptim;
	time_t finalquery_start_date;
	int loop_interval=DEFAULT_LOOP_INTERVAL;
	
	int rc;				
	int c;				
	
	pthread_t RecUpdNetThd;

	int confirm_time=0;	

        static int help;
        static int short_help;
	
	bact.njobs = 0;
	bact.jobs = NULL;
	
	while (1) {
		static struct option long_options[] =
		{
		{"help",      no_argument,     &help,       1},
		{"usage",     no_argument,     &short_help, 1},
		{"nodaemon",  no_argument,       0, 'o'},
		{"version",   no_argument,       0, 'v'},
		{"prefix",    required_argument, 0, 'p'},
		{0, 0, 0, 0}
		};

		int option_index = 0;
     
		c = getopt_long (argc, argv, "vop:",long_options, &option_index);
     
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

		case 'p':
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
	
        ret = config_get("slurm_binpath",cha);
        if (ret == NULL){
                do_log(debuglogfile, debug, 1, "%s: key slurm_binpath not found\n",argv0);
        } else {
                slurm_binpath=strdup(ret->value);
                if(slurm_binpath == NULL){
                        sysfatal("strdup failed for slurm_binpath in main: %r");
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
	
	ret = config_get("bupdater_consistency_check_interval",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key bupdater_consistency_check_interval not found using the default:%d\n",argv0,bupdater_consistency_check_interval);
	} else {
		bupdater_consistency_check_interval=atoi(ret->value);
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
	
	ret = config_get("job_registry_use_mmap",cha);
	if (ret == NULL){
		do_log(debuglogfile, debug, 1, "%s: key job_registry_use_mmap not found. Default is NO\n",argv0);
	} else {
		do_log(debuglogfile, debug, 1, "%s: key job_registry_use_mmap is set to %s\n",argv0,ret->value);
	}
	
	remupd_conf = config_get("job_registry_add_remote",cha);
	if (remupd_conf == NULL){
		do_log(debuglogfile, debug, 1, "%s: key job_registry_add_remote not found\n",argv0);
	}else{
		if (job_registry_updater_setup_receiver(remupd_conf->values,remupd_conf->n_values,&remupd_head) < 0){
			do_log(debuglogfile, debug, 1, "%s: Cannot set network receiver(s) up for remote update\n",argv0);
			fprintf(stderr,"%s: Cannot set network receiver(s) up for remote update \n",argv0);
       		}
 
		if (remupd_head == NULL){
			do_log(debuglogfile, debug, 1, "%s: Cannot find values for network endpoints in configuration file (attribute 'job_registry_add_remote').\n",argv0);
			fprintf(stderr,"%s: Cannot find values for network endpoints in configuration file (attribute 'job_registry_add_remote').\n", argv0);
		}

		if ((remupd_nfds = job_registry_updater_get_pollfd(remupd_head, &remupd_pollset)) < 0){
			do_log(debuglogfile, debug, 1, "%s: Cannot setup poll set for receiving data.\n",argv0);
    			fprintf(stderr,"%s: Cannot setup poll set for receiving data.\n", argv0);
		}
		if (remupd_pollset == NULL || remupd_nfds == 0){
			do_log(debuglogfile, debug, 1, "%s: No poll set available for receiving data.\n",argv0);
			fprintf(stderr,"%s: No poll set available for receiving data.\n",argv0);
		}
	
	}
	
	if( !nodmn ) daemonize();


	if( pidfile ){
		writepid(pidfile);
		free(pidfile);
	}
	
	rha=job_registry_init(registry_file, BY_BATCH_ID);
	if (rha == NULL){
		do_log(debuglogfile, debug, 1, "%s: Error initialising job registry %s\n",argv0,registry_file);
		fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
		perror("");
	}
	
	if (remupd_conf != NULL){
		pthread_create(&RecUpdNetThd, NULL, (void *(*)(void *))ReceiveUpdateFromNetwork, (void *)NULL);
	
		if (job_registry_updater_setup_sender(remupd_conf->values,remupd_conf->n_values,0,&remupd_head_send) < 0){
			do_log(debuglogfile, debug, 1, "%s: Cannot set network sender(s) up for remote update\n",argv0);
			fprintf(stderr,"%s: Cannot set network sender(s) up for remote update \n",argv0);
       		}
		if (remupd_head_send == NULL){
			do_log(debuglogfile, debug, 1, "%s: Cannot find values for network endpoints in configuration file (attribute 'job_registry_add_remote').\n",argv0);
			fprintf(stderr,"%s: Cannot find values for network endpoints in configuration file (attribute 'job_registry_add_remote').\n", argv0);
		}
	}

	config_free(cha);
	
	for(;;){
		/* Purge old entries from registry */
		now=time(0);
		if(now - purge_time > 86400){
			if((rc=job_registry_purge(registry_file, now-purge_interval,0))<0){
				do_log(debuglogfile, debug, 1, "%s: Error purging job registry %s:%d\n",argv0,registry_file,rc);
                	        fprintf(stderr,"%s: Error purging job registry %s :",argv0,registry_file);
                	        perror("");

			}
			purge_time=time(0);
		}
		
		now=time(0);
		if(now - last_consistency_check > bupdater_consistency_check_interval){
			if(job_registry_check_index_key_uniqueness(rha,&first_duplicate)==JOB_REGISTRY_FAIL){
				do_log(debuglogfile, debug, 1, "%s: Found job registry duplicate entry. The first one is:%s\n.Jobid should be removed or registry directory should be removed.\n",argv0,first_duplicate);
               	        	fprintf(stderr,"%s: Found job registry duplicate entry. The first one is:%s.\nJobid should be removed or registry directory should be removed.",argv0,first_duplicate);
 
			}
			last_consistency_check=time(0);
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
		finalquery_start_date = time(0);
		
		while ((en = job_registry_get_next(rha, fd)) != NULL){

			if((bupdater_lookup_active_jobs(&bact,en->batch_id) != BUPDATER_ACTIVE_JOBS_SUCCESS) && en->status!=REMOVED && en->status!=COMPLETED){

				confirm_time=atoi(en->updater_info);
				if(confirm_time==0){
					confirm_time=en->mdate;
				}
			
				/* Assign Status=4 and ExitStatus=999 to all entries that after alldone_interval are still not in a final state(3 or 4)*/
				if(now-confirm_time>alldone_interval){
					AssignFinalState(en->batch_id);
					free(en);
					continue;
				}
								
				if(en->status==IDLE && strlen(en->updater_info)>0){
					if (en->mdate < finalquery_start_date){
						finalquery_start_date=en->mdate;
					}
					do_log(debuglogfile, debug, 2, "%s: FinalStateQuery needed for jobid=%s with status=%d\n",argv0,en->batch_id,en->status);
					runfinal=TRUE;
				}else if((now-confirm_time>finalstate_query_interval) && (now > next_finalstatequery)){
					if (en->mdate < finalquery_start_date){
						finalquery_start_date=en->mdate;
					}
					do_log(debuglogfile, debug, 2, "%s: FinalStateQuery needed for jobid=%s with status=%d\n",argv0,en->batch_id,en->status);
					runfinal=TRUE;
				}
				
			
			}
			free(en);
		}
		
		if(runfinal_oldlogs){
			FinalStateQuery(0,1);
			runfinal_oldlogs=FALSE;
			runfinal=FALSE;
		}else if(runfinal){
			FinalStateQuery(finalquery_start_date,1);
			runfinal=FALSE;
		}
		fclose(fd);		
		sleep(loop_interval);
	}
	
	job_registry_destroy(rha);
	
	return 0;
	
}

int 
ReceiveUpdateFromNetwork()
{
	char *proxy_path, *proxy_subject;
	int timeout_ms = 0;
	int ret, prret, rhret;
	job_registry_entry *nen;
	job_registry_entry *ren;
  
	proxy_path = NULL;
	proxy_subject = NULL;
	
	while ((nen = job_registry_receive_update(remupd_pollset, remupd_nfds,timeout_ms, &proxy_subject, &proxy_path))){
	
		JOB_REGISTRY_ASSIGN_ENTRY(nen->subject_hash,"\0");
		JOB_REGISTRY_ASSIGN_ENTRY(nen->proxy_link,"\0");
		
		if ((ren=job_registry_get(rha, nen->batch_id)) == NULL){
			if ((ret=job_registry_append(rha, nen)) < 0){
				fprintf(stderr,"%s: Warning: job_registry_append returns %d: ",argv0,ret);
				perror("");
			} 
		}else{
		
			if(ren->subject_hash!=NULL && strlen(ren->subject_hash) && ren->proxy_link!=NULL && strlen(ren->proxy_link)){
				JOB_REGISTRY_ASSIGN_ENTRY(nen->subject_hash,ren->subject_hash);
				JOB_REGISTRY_ASSIGN_ENTRY(nen->proxy_link,ren->proxy_link);
			}else{
				if (proxy_path != NULL && strlen(proxy_path) > 0){
					prret = job_registry_set_proxy(rha, nen, proxy_path);
     			 		if (prret < 0){
						do_log(debuglogfile, debug, 1, "%s: warning: setting proxy to %s\n",argv0,proxy_path);
        					fprintf(stderr,"%s: warning: setting proxy to %s: ",argv0,proxy_path);
        					perror("");
        					/* Make sure we don't renew non-existing proxies */
						nen->renew_proxy = 0;  		
					}
					free(proxy_path);
  
					nen->subject_hash[0] = '\000';
					if (proxy_subject != NULL && strlen(proxy_subject) > 0){
						job_registry_compute_subject_hash(nen, proxy_subject);
						rhret = job_registry_record_subject_hash(rha, nen->subject_hash, proxy_subject, TRUE);  
						if (rhret < 0){
							do_log(debuglogfile, debug, 1, "%s: warning: recording proxy subject %s (hash %s)\n",argv0, proxy_subject, nen->subject_hash);
							fprintf(stderr,"%s: warning: recording proxy subject %s (hash %s): ",argv0, proxy_subject, nen->subject_hash);
							perror("");
						}
					}
					free(proxy_subject);
  
				}
			}
			if(job_registry_need_update(ren,nen,JOB_REGISTRY_UPDATE_ALL)){
				if ((ret=job_registry_update(rha, nen)) < 0){
					fprintf(stderr,"%s: Warning: job_registry_update returns %d: ",argv0,ret);
					perror("");
				}
			} 
		}
		free(nen);
	}
  
	return 0;
}

int
IntStateQuery()
{

        FILE *fp;
	char *line=NULL;
	char **token;
	char **token_l;
	char **token_e;
	int maxtok_t=0;
	int maxtok_l=0;
	int maxtok_e=0;
	job_registry_entry en;
	int ret;
	time_t tmstampepoch;
	char *cp=NULL; 
	char *batch_str=NULL;
	char *command_string=NULL;
	job_registry_entry *ren=NULL;
	int isresumed=FALSE;
	int first=TRUE;
	time_t now;
	char *string_now=NULL;

	command_string=make_message("%s/scontrol -a show jobid",slurm_binpath);
	fp = popen(command_string,"r");

	en.status=UNDEFINED;
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
	JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
	JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,"\0");
	en.exitcode=-1;
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
			now=time(0);
			string_now=make_message("%d",now);
			maxtok_t = strtoken(line, ' ', &token);
			if(line && strstr(line,"JobId=")){
				isresumed=FALSE;
				if(!first && en.status!=UNDEFINED && ren && ren->status!=REMOVED && ren->status!=COMPLETED){	
					if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum,
					JOB_REGISTRY_UPDATE_WN_ADDR|
					JOB_REGISTRY_UPDATE_STATUS|
					JOB_REGISTRY_UPDATE_UDATE|
					JOB_REGISTRY_UPDATE_UPDATER_INFO|
					JOB_REGISTRY_UPDATE_EXITCODE|
					JOB_REGISTRY_UPDATE_EXITREASON)) < 0){
						if(ret != JOB_REGISTRY_NOT_FOUND){
							fprintf(stderr,"Update of record returns %d: ",ret);
							perror("");
						}
					} else {
						if(ret==JOB_REGISTRY_SUCCESS){
							if (en.status == REMOVED || en.status == COMPLETED) {
								do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d exitcode=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status,en.exitcode);
								job_registry_unlink_proxy(rha, &en);
							}else{
								do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
							}
							if (remupd_conf != NULL){
								if ((ret=job_registry_send_update(remupd_head_send,&en,NULL,NULL))<=0){
									do_log(debuglogfile, debug, 2, "%s: Error creating endpoint in IntStateQuery\n",argv0);
								}
							}
						}
					}
					en.status = UNDEFINED;
					JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
					JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,"\0");
					JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
					en.exitcode=-1;
				}
				en.status = UNDEFINED;
				maxtok_l = strtoken(token[0], '=', &token_l);
				batch_str=strdup(token_l[1]);
				JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_str);
				JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
				en.exitcode=-1;
				bupdater_push_active_job(&bact, en.batch_id);
				free(batch_str);
				freetoken(&token_l,maxtok_l);
				if(!first) free(ren);
				if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
						fprintf(stderr,"Get of record returns error ");
						perror("");
				}
				if(ren){
					if(strlen(ren->updater_info)>0){
						en.udate=ren->udate;
					}else{
						en.udate=time(0);
					}
				}
				first=FALSE;
				
			}else if(line && strstr(line," JobState=")){
				if(token[0] && strstr(line,"JobState=")){
					maxtok_l = strtoken(token[0], '=', &token_l);
					if(token_l[1] && strstr(token_l[1],"PENDING")){
						en.status=IDLE;
						en.exitcode=-1;
						JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
					}else if(token_l[1] && strstr(token_l[1],"RUNNING")){
						en.status=RUNNING;
						en.exitcode=-1;
						JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
					}else if(token_l[1] && strstr(token_l[1],"COMPLETED")){
						en.status=COMPLETED;
						en.exitcode=0;
						JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
					}else if(token_l[1] && strstr(token_l[1],"CANCELLED")){
						en.status=REMOVED;
						en.exitcode=-999;
						JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
					}else if(token_l[1] && strstr(token_l[1],"FAILED")){
						en.status=COMPLETED;
						en.exitcode=0;
						JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
					}else if(token_l[1] && strstr(token_l[1],"SUSPENDED")){
						en.status=HELD;
						en.exitcode=-1;
						JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
					}else if(token_l[1] && strstr(token_l[1],"COMPLETING")){
						bupdater_remove_active_job(&bact, en.batch_id);
					}
					freetoken(&token_l,maxtok_l);
				}
			}else if(line && strstr(line," BatchHost=")){
				if(token[0] && strstr(line,"BatchHost=")){
					maxtok_l = strtoken(token[0], '=', &token_l);
					if(en.status!=IDLE){
						JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,token_l[1]);
					}
					freetoken(&token_l,maxtok_l);
				}
			}else if(line && strstr(line," ExitCode=")){
				if(token[3] && strstr(line,"ExitCode=")){
					maxtok_l = strtoken(token[3], '=', &token_l);
					maxtok_e = strtoken(token_l[1], ':', &token_e);
					if(en.status==COMPLETED){
						en.exitcode=atoi(token_e[0]);
					}
					freetoken(&token_l,maxtok_l);
					freetoken(&token_e,maxtok_e);
				}
			}else if(line && strstr(line," SubmitTime=")){
				if(en.status==IDLE){
					if(token[0] && strstr(line,"SubmitTime=")){
						maxtok_l = strtoken(token[0], '=', &token_l);
						tmstampepoch=str2epoch(token_l[1],"N");
						en.udate=tmstampepoch;
						freetoken(&token_l,maxtok_l);
					}
				}
			}else if(line && strstr(line," StartTime=")){
				if(en.status==RUNNING){
					if(token[0] && strstr(line,"StartTime=")){
						maxtok_l = strtoken(token[0], '=', &token_l);
						tmstampepoch=str2epoch(token_l[1],"N");
						en.udate=tmstampepoch;
						freetoken(&token_l,maxtok_l);
					}
				}
				if(en.status==COMPLETED || en.status==REMOVED){
					if(token[1] && strstr(line,"EndTime=")){
						maxtok_l = strtoken(token[1], '=', &token_l);
						tmstampepoch=str2epoch(token_l[1],"N");
						en.udate=tmstampepoch;
						freetoken(&token_l,maxtok_l);
					}
				}
			}else if(line && strstr(line," SuspendTime=")){
				if(en.status==HELD){
					if(token[1] && strstr(line,"SuspendTime=")){
						maxtok_l = strtoken(token[1], '=', &token_l);
						tmstampepoch=str2epoch(token_l[1],"N");
						en.udate=tmstampepoch;
						freetoken(&token_l,maxtok_l);
					}
				}
			}
			
			free(line);
			free(string_now);
			freetoken(&token,maxtok_t);
		}
		pclose(fp);
	}
		
	if(en.status!=UNDEFINED && ren && ren->status!=REMOVED && ren->status!=COMPLETED){	
		if ((ret=job_registry_update_recn_select(rha, &en, ren->recnum,
		JOB_REGISTRY_UPDATE_WN_ADDR|
		JOB_REGISTRY_UPDATE_STATUS|
		JOB_REGISTRY_UPDATE_UDATE|
		JOB_REGISTRY_UPDATE_UPDATER_INFO|
		JOB_REGISTRY_UPDATE_EXITCODE|
		JOB_REGISTRY_UPDATE_EXITREASON)) < 0){
			if(ret != JOB_REGISTRY_NOT_FOUND){
				fprintf(stderr,"Update of record returns %d: ",ret);
				perror("");
			}
		} else {
			if(ret==JOB_REGISTRY_SUCCESS){
				if (en.status == REMOVED || en.status == COMPLETED) {
					do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d exitcode=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status,en.exitcode);
					job_registry_unlink_proxy(rha, &en);
				}else{
					do_log(debuglogfile, debug, 2, "%s: registry update in IntStateQuery for: jobid=%s creamjobid=%s wn=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.wn_addr,en.status);
				}
				if (remupd_conf != NULL){
					if ((ret=job_registry_send_update(remupd_head_send,&en,NULL,NULL))<=0){
						do_log(debuglogfile, debug, 2, "%s: Error creating endpoint in IntStateQuery\n",argv0);
					}
				}
			}
		}
	}				

	free(ren);
	free(command_string);
	return 0;
}

int
FinalStateQuery(time_t start_date, int logs_to_read)
{

        FILE *fp;
	char *line=NULL;
	char **token;
	char **token_l;
	int maxtok_t=0;
	int maxtok_l=0;
	job_registry_entry en;
	int ret;
	time_t tmstampepoch;
	char *cp=NULL; 
	char *command_string=NULL;
	time_t now;
	char *string_now=NULL;
	job_registry_entry *ren=NULL;

	
	command_string=make_message("%s/sacct -nap -o JobID,JobName,State,ExitCode,submit,start,end 2>/dev/null",slurm_binpath);
	
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
			now=time(0);
			string_now=make_message("%d",now);
			maxtok_t = strtoken(line, '|', &token);
			JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,token[0]);
			JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
			if(token[2] && strstr(token[2],"COMPLETED")){
				en.status=COMPLETED;
				JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
			}else if(token[2] && strstr(token[2],"CANCELLED")){
				en.status=REMOVED;
				en.exitcode=-999;
				JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
			}else if(token[2] && strstr(token[2],"FAILED")){
				en.status=COMPLETED;
				JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now);
			}
			
			tmstampepoch=str2epoch(token[6],"N");
			en.udate=tmstampepoch;
			if(en.status==COMPLETED){
				maxtok_l = strtoken(token[3], ':', &token_l);
				en.exitcode=atoi(token_l[0]);
				freetoken(&token_l,maxtok_l);
			}
			
			if ((ren=job_registry_get(rha, en.batch_id)) == NULL){
					fprintf(stderr,"Get of record returns error ");
					perror("");
			}
			if(en.status!=UNDEFINED && en.status!=IDLE && ren && ren->status!=REMOVED && ren->status!=COMPLETED){	
				if ((ret=job_registry_update_select(rha, &en,
				JOB_REGISTRY_UPDATE_UDATE |
				JOB_REGISTRY_UPDATE_STATUS |
				JOB_REGISTRY_UPDATE_UPDATER_INFO |
				JOB_REGISTRY_UPDATE_EXITCODE |
				JOB_REGISTRY_UPDATE_EXITREASON )) < 0){
					if(ret != JOB_REGISTRY_NOT_FOUND){
						fprintf(stderr,"Update of record returns %d: ",ret);
						perror("");
					}
				} else {
					do_log(debuglogfile, debug, 2, "%s: f registry update in FinalStateQuery for: jobid=%s creamjobid=%s status=%d\n",argv0,en.batch_id,en.user_prefix,en.status);
					if (en.status == REMOVED || en.status == COMPLETED){
						job_registry_unlink_proxy(rha, &en);
					}
					if (remupd_conf != NULL){
						if ((ret=job_registry_send_update(remupd_head_send,&en,NULL,NULL))<=0){
							do_log(debuglogfile, debug, 2, "%s: Error creating endpoint in FinalStateQuery\n",argv0);
						}
					}
				}
			}
			free(string_now);
			free(line);
			freetoken(&token,maxtok_t);
			free(ren);
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
		if (remupd_conf != NULL){
			if ((ret=job_registry_send_update(remupd_head_send,&en,NULL,NULL))<=0){
				do_log(debuglogfile, debug, 2, "%s: Error creating endpoint in AssignFinalState\n",argv0);
			}
		}
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
	printf("Usage: BUpdaterSLURM [OPTION...]\n");
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
	printf("Usage: BUpdaterSLURM [-ov?] [-o|--nodaemon] [-v|--version] [-?|--help] [--usage]\n");
	exit(EXIT_SUCCESS);
}

