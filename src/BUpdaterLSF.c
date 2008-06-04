#include "BUpdaterLSF.h"

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
	time_t dgbtimestamp;
	
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
    
	if(debug){
		if((debuglogfile = fopen(debuglogname, "a+"))==0){
			debuglogfile =  fopen("/dev/null", "a+");
		}
	}
	
        ret = config_get("lsf_binpath",cha);
        if (ret == NULL){
                if(debug){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: key lsf_binpath not found\n",iepoch2str(dgbtimestamp),argv0);
			fflush(debuglogfile);
		}
        } else {
                lsf_binpath=strdup(ret->value);
        }
	
	ret = config_get("job_registry",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: key job_registry not found\n",iepoch2str(dgbtimestamp),argv0);
			fflush(debuglogfile);
		}
	} else {
		registry_file=strdup(ret->value);
	}
	
	ret = config_get("purge_interval",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: key purge_interval not found using the default:%d\n",iepoch2str(dgbtimestamp),argv0,purge_interval);
			fflush(debuglogfile);
		}
	} else {
		purge_interval=atoi(ret->value);
	}
	
	ret = config_get("finalstate_query_interval",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: key finalstate_query_interval not found using the default:%d\n",iepoch2str(dgbtimestamp),argv0,finalstate_query_interval);
			fflush(debuglogfile);
		}
	} else {
		finalstate_query_interval=atoi(ret->value);
	}
	
	ret = config_get("alldone_interval",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: key alldone_interval not found using the default:%d\n",iepoch2str(dgbtimestamp),argv0,alldone_interval);
			fflush(debuglogfile);
		}
	} else {
		alldone_interval=atoi(ret->value);
	}
	
	ret = config_get("bhist_logs_to_read",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: key bhist_logs_to_read not found using the default:%d\n",iepoch2str(dgbtimestamp),argv0,bhist_logs_to_read);
			fflush(debuglogfile);
		}
	} else {
		bhist_logs_to_read=atoi(ret->value);
	}
	
	ret = config_get("bupdater_pidfile",cha);
	if (ret == NULL){
                if(debug){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: key bupdater_pidfile not found\n",iepoch2str(dgbtimestamp),argv0);
			fflush(debuglogfile);
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
					dgbtimestamp=time(0);
					fprintf(debuglogfile, "%s %s: Error purging job registry %s\n",iepoch2str(dgbtimestamp),argv0,registry_file);
					fflush(debuglogfile);
				}
                	        fprintf(stderr,"%s: Error purging job registry %s :",argv0,registry_file);
                	        perror("");
				sleep(2);

			}else{
				purge_time=time(0);
			}
		}
	       
		rha=job_registry_init(registry_file, BY_BATCH_ID);
		if (rha == NULL){
			if(debug){
				dgbtimestamp=time(0);
				fprintf(debuglogfile, "%s %s: Error initialising job registry %s\n",iepoch2str(dgbtimestamp),argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
			perror("");
			sleep(2);
			continue;
		}

		IntStateQuery();
		
		fd = job_registry_open(rha, "r");
		if (fd == NULL){
			if(debug){
				dgbtimestamp=time(0);
				fprintf(debuglogfile, "%s %s: Error opening job registry %s\n",iepoch2str(dgbtimestamp),argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
			perror("");
			sleep(2);
			continue;
		}
		if (job_registry_rdlock(rha, fd) < 0){
			if(debug){
				dgbtimestamp=time(0);
				fprintf(debuglogfile, "%s %s: Error read locking job registry %s\n",iepoch2str(dgbtimestamp),argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error read locking job registry %s :",argv0,registry_file);
			perror("");
			sleep(2);
			continue;
		}

		first=TRUE;
		
		while ((en = job_registry_get_next(rha, fd)) != NULL){

			if((bupdater_lookup_active_jobs(&bact,en->batch_id) != BUPDATER_ACTIVE_JOBS_SUCCESS) && en->status!=REMOVED && en->status!=COMPLETED){
				/* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4)*/
				if(now-en->mdate>alldone_interval){
					AssignFinalState(en->batch_id);
					free(en);
					continue;
				}
			
				if(now-en->mdate>finalstate_query_interval){
					runfinal=TRUE;
				}
			}
			free(en);
		}
		
		if(runfinal){
			FinalStateQuery();
			runfinal=FALSE;
		}
		fclose(fd);		
		job_registry_destroy(rha);
		sleep(2);
	}
	
	return(0);
	
}


int
IntStateQuery()
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
	char *line;
	char **token;
	int maxtok_l=0,i;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	time_t dgbtimestamp;
	char *tmp; 
	char *cp; 

	if((token=calloc(NUMTOK * sizeof *token,1)) == 0){
		sysfatal("can't malloc token %r");
	}
	if((command_string=malloc(strlen(lsf_binpath) + 17)) == 0){
		sysfatal("can't malloc command_string %r");
	}
	
	sprintf(command_string,"%s/bjobs -u all -w",lsf_binpath);
	fp = popen(command_string,"r");

	en.status=UNDEFINED;
	en.wn_addr[0]='\0';
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
			if(en.status!=UNDEFINED){	
				if(debug>1){
					dgbtimestamp=time(0);
					fprintf(debuglogfile, "%s %s: registry update in IntStateQuery for: jobid=%s wn=%s status=%d\n",iepoch2str(dgbtimestamp),argv0,en.batch_id,en.wn_addr,en.status);
					fflush(debuglogfile);
				}
				if ((ret=job_registry_update(rha, &en)) < 0){
					if(ret != JOB_REGISTRY_NOT_FOUND){
						fprintf(stderr,"Append of record returns %d: ",ret);
						perror("");
					}
				}
				en.status = UNDEFINED;
			}
				
			JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,tmp);
			bupdater_push_active_job(&bact, en.batch_id);
			free(tmp);
			
			maxtok_l = strtoken(line, ' ', token, NUMTOK);
			
			JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,token[5]);
			
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
		
			for(i=0;i<maxtok_l;i++){
				free(token[i]);
			}
			
			free(line);
		}
		pclose(fp);
	}
	
	if(en.status!=UNDEFINED){	
		if(debug>1){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: registry update in IntStateQuery for: jobid=%s wn=%s status=%d\n",iepoch2str(dgbtimestamp),argv0,en.batch_id,en.wn_addr,en.status);
			fflush(debuglogfile);
		}
		if ((ret=job_registry_update(rha, &en)) < 0){
			if(ret != JOB_REGISTRY_NOT_FOUND){
				fprintf(stderr,"Append of record returns %d: ",ret);
				perror("");
			}
		}
	}				

	free(token);
	free(command_string);
	return(0);
}

int
FinalStateQuery()
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
	char *line;
	char **token;
	int maxtok_t=0,j;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *batch_str;
	char *ex_str; 
	char *cp; 
	time_t dgbtimestamp;

	if((token=calloc(NUMTOK * sizeof *token,1)) == 0){
		sysfatal("can't malloc token %r");
	}
	if((command_string=malloc(strlen(lsf_binpath) + NUM_CHARS + 24)) == 0){
		sysfatal("can't malloc command_string %r");
	}

	sprintf(command_string,"%s/bhist -u all -d -e -l -n %d",lsf_binpath,bhist_logs_to_read);
	fp = popen(command_string,"r");

	en.status=UNDEFINED;

	if(fp!=NULL){
		while(!feof(fp) && (line=get_line(fp))){
			if(line && strlen(line)==0){
				free(line);
				continue;
			}
			if ((cp = strrchr (line, '\n')) != NULL){
				*cp = '\0';
			}
				
			if(line && strstr(line,"Job <")){	
				if(en.status!=UNDEFINED){	
                        		if ((ret=job_registry_update(rha, &en)) < 0){
						if(ret != JOB_REGISTRY_NOT_FOUND){
                	                		fprintf(stderr,"Append of record returns %d: ",ret);
							perror("");
						}
						en.status = UNDEFINED;
					}
				}				
				maxtok_t = strtoken(line, ',', token, NUMTOK);
				batch_str=strdel(token[0],"Job <>");
				JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_str);
				free(batch_str);
				for(j=0;j<maxtok_t;j++){
					free(token[j]);
				}
			}else if(line && strstr(line," Exited with exit code")){	
				maxtok_t = strtoken(line, ' ', token, NUMTOK);
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
				for(j=0;j<maxtok_t;j++){
					free(token[j]);
				}
			}else if(line && strstr(line," Done successfully")){	
				maxtok_t = strtoken(line, ' ', token, NUMTOK);
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
				for(j=0;j<maxtok_t;j++){
					free(token[j]);
				}
			}else if(line && strstr(line," Signal <KILL>")){	
				maxtok_t = strtoken(line, ' ', token, NUMTOK);
                        	if((timestamp=malloc(strlen(token[0]) + strlen(token[1]) + strlen(token[2]) + strlen(token[3]) + 4)) == 0){
					sysfatal("can't malloc timestamp in FinalStateQuery: %r");
				}
				sprintf(timestamp,"%s %s %s %s",token[0],token[1],token[2],token[3]);
				timestamp[strlen(timestamp)-1]='\0';
				tmstampepoch=str2epoch(timestamp,"W");
				en.udate=tmstampepoch;
				en.status=REMOVED;
				free(timestamp);
				JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
				for(j=0;j<maxtok_t;j++){
					free(token[j]);
				}
			}
			free(line);
		}
		pclose(fp);
	}
	
	if(en.status!=UNDEFINED){	
		if(debug>1){
			dgbtimestamp=time(0);
			fprintf(debuglogfile, "%s %s: registry update in FinalStateQuery for: jobid=%s wn=%s status=%d\n",iepoch2str(dgbtimestamp),argv0,en.batch_id,en.wn_addr,en.status);
			fflush(debuglogfile);
		}
		if ((ret=job_registry_update(rha, &en)) < 0){
			if(ret != JOB_REGISTRY_NOT_FOUND){
				fprintf(stderr,"Append of record returns %d: ",ret);
				perror("");
			}
		}
	}				

	free(token);
	free(command_string);
	return(0);
}

int AssignFinalState(char *batchid){

	job_registry_entry en;
	int ret,i;
	time_t now;

	now=time(0);
	
	JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batchid);
	en.status=COMPLETED;
	en.exitcode=-1;
	en.udate=now;
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
	JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
		
	if ((ret=job_registry_update(rha, &en)) < 0){
		if(ret != JOB_REGISTRY_NOT_FOUND){
			fprintf(stderr,"Append of record %d returns %d: ",i,ret);
			perror("");
		}
	}
	return(0);
}
