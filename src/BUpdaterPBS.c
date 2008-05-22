#include "BUpdaterPBS.h"

int main(int argc, char *argv[]){

	FILE *fd;
	job_registry_entry *en;
	time_t now;
	time_t purge_time=0;
	char *q=NULL;
	char *pidfile=NULL;
	char *final_string=NULL;
	
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
	
        ret = config_get("pbs_binpath",cha);
        if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key pbs_binpath not found\n",argv0);
			fflush(debuglogfile);
		}
        } else {
                pbs_binpath=strdup(ret->value);
        }
	
	ret = config_get("job_registry",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key job_registry not found\n",argv0);
			fflush(debuglogfile);
		}
	} else {
		registry_file=strdup(ret->value);
	}
	
	ret = config_get("purge_interval",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key purge_interval not found using the default:%d\n",argv0,purge_interval);
			fflush(debuglogfile);
		}
	} else {
		purge_interval=atoi(ret->value);
	}
	
	ret = config_get("finalstate_query_interval",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key finalstate_query_interval not found using the default:%d\n",argv0,finalstate_query_interval);
			fflush(debuglogfile);
		}
	} else {
		finalstate_query_interval=atoi(ret->value);
	}
	
	ret = config_get("alldone_interval",cha);
	if (ret == NULL){
                if(debug){
			fprintf(debuglogfile, "%s: key alldone_interval not found using the default:%d\n",argv0,alldone_interval);
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
	       
		rha=job_registry_init(registry_file, BY_BATCH_ID);
		if (rha == NULL)
		{
			if(debug){
				fprintf(debuglogfile, "%s: Error initialising job registry %s\n",argv0,registry_file);
				fflush(debuglogfile);
			}
			fprintf(stderr,"%s: Error initialising job registry %s :",argv0,registry_file);
			perror("");
			sleep(2);
			continue;
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

		if((final_string=calloc(STR_CHARS,1)) == 0){
			sysfatal("can't malloc constraint %r");
        	}
		first=TRUE;
		
		while ((en = job_registry_get_next(rha, fd)) != NULL)
		{

			/* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4)*/
			if((now-en->mdate>alldone_interval) && en->status!=REMOVED && en->status!=COMPLETED)
			{
				AssignFinalState(en->batch_id);	
			}
			
			if((now-en->mdate>finalstate_query_interval) && en->status!=REMOVED && en->status!=COMPLETED)
			{
				strcat(final_string,en->batch_id);
				strcat(final_string,":");
				runfinal=TRUE;
			}
			free(en);
		}
		
		if(runfinal){
			FinalStateQuery(final_string);
			runfinal=FALSE;
		}
		free(final_string);		
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
 exitcode
 udate
 
 Filled by submit script:
 blah_id 
 
 Unfilled entries:
 exitreason
*/


	char *output;
        FILE *file_output;
	int len;
	char **line;
	char **token;
	int maxtok_l=0,maxtok_t=0,i,j;
	job_registry_entry en;
	int ret;
	int retcode;
	char *timestamp;
	int tmstampepoch;
	char *batch_str;
	char *wn_str; 
        char *twn_str;
        char *status_str;

/*
        if((output=calloc(STR_CHARS,1)) == 0){
                printf("can't malloc output\n");
        }
*/
	if((line=calloc(10000 * sizeof *line,1)) == 0){
		sysfatal("can't malloc line %r");
	}
	if((token=calloc(10000 * sizeof *token,1)) == 0){
		sysfatal("can't malloc token %r");
	}
	if((command_string=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc command_string %r");
	}
		
	sprintf(command_string,"%s/qstat -f",pbs_binpath);
	retcode = exe_getout(command_string, NULL, &output);
	
	/*
	file_output = popen(command_string,"r");

	if (file_output != NULL){
		len = fread(output, sizeof(char), STR_CHARS - 1 , file_output);
		if (len>0){
			output[len-1]='\000';
		}
		pclose(file_output);
	}
	*/
	en.status=UNDEFINED;

	maxtok_l = strtoken(output, '\n', line);
	
	if(output)free(output);

	for(i=0;i<maxtok_l;i++){
		if(line[i] && strstr(line[i],"Job Id: ")){
			if(en.status!=UNDEFINED){	
                        	if ((ret=job_registry_update(rha, &en)) < 0){
                	                fprintf(stderr,"Append of record returns %d: ",ret);
					perror("");
				}
			}				
                        maxtok_t = strtoken(line[i], ':', token);
			batch_str=strdel(token[1]," ");
			JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,batch_str);
			free(batch_str);
                        for(j=0;j<maxtok_t;j++){
                                free(token[j]);
                        }
		}
		if(line[i] && strstr(line[i],"job_state = ")){	
			maxtok_t = strtoken(line[i], '=', token);
			status_str=strdel(token[1]," ");
			if(status_str && strcmp(status_str,"Q")==0){ 
				en.status=IDLE;
			}
			if(status_str && strcmp(status_str,"R")==0){ 
				en.status=RUNNING;
			}
			if(status_str && strcmp(status_str,"H")==0){ 
				en.status=HELD;
			}
			free(status_str);
                        for(j=0;j<maxtok_t;j++){
                                free(token[j]);
                        }
		}
		if(line[i] && strstr(line[i],"exec_host = ")){	
			maxtok_t = strtoken(line[i], '=', token);
			twn_str=strdup(token[1]);
                        for(j=0;j<maxtok_t;j++){
                                free(token[j]);
                        }
			maxtok_t = strtoken(twn_str, '/', token);
			wn_str=strdel(token[0]," ");
			JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,wn_str);
			free(twn_str);
 			free(wn_str);
                       for(j=0;j<maxtok_t;j++){
                                free(token[j]);
                        }
		}
		if(line[i] && strstr(line[i],"ctime = ")){	
                        maxtok_t = strtoken(line[i], ' ', token);
                        if((timestamp=calloc(STR_CHARS,1)) == 0){
                                sysfatal("can't malloc wn in PollDB: %r");
                        }
                        sprintf(timestamp,"%s %s %s %s %s",token[2],token[3],token[4],token[5],token[6]);
                        tmstampepoch=str2epoch(timestamp,"L");
			free(timestamp);
			en.udate=tmstampepoch;
                        for(j=0;j<maxtok_t;j++){
                                free(token[j]);
                        }
		}
	}
	if(en.status!=UNDEFINED){	
		if ((ret=job_registry_update(rha, &en)) < 0){
			fprintf(stderr,"Append of record returns %d: ",ret);
			perror("");
		}
	}				

	for(i=0;i<maxtok_l;i++){
		free(line[i]);
	}
	free(line);
	free(token);
	free(command_string);
	return(0);
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
	char *output;
        FILE *file_output;
	int len;
	char **line;
	char **token;
	char **jobid;
	int maxtok_l=0,maxtok_t=0,maxtok_j=0,i,j,k;
	job_registry_entry en;
	int ret;
	char *timestamp;
	int tmstampepoch;
	char *batch_str;
	char *wn_str; 
	char *exit_str; 

        if((output=calloc(STR_CHARS,1)) == 0){
                printf("can't malloc output\n");
        }
	if((line=calloc(10000 * sizeof *line,1)) == 0){
		sysfatal("can't malloc line %r");
	}
	if((token=calloc(10000 * sizeof *token,1)) == 0){
		sysfatal("can't malloc token %r");
	}
	if((jobid=calloc(10000 * sizeof *jobid,1)) == 0){
		sysfatal("can't malloc jobid %r");
	}
	if((command_string=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc command_string %r");
	}
	
	if(debug>1){
		fprintf(debuglogfile, "%s: jobid string in FinalStateQuery is:%s\n",argv0,input_string);
		fflush(debuglogfile);
	}
	
	maxtok_j = strtoken(input_string, ':', jobid);
	
	for(k=0;k<maxtok_j;k++){
	
		if(strlen(jobid[k])==0){
			continue;
		}
		sprintf(command_string,"%s/tracejob -m -l -a %s",pbs_binpath,jobid[k]);
		file_output = popen(command_string,"r");
		
		if(debug>1){
			fprintf(debuglogfile, "%s: command_string in FinalStateQuery is:%s\n",argv0,command_string);
			fflush(debuglogfile);
		}

		if (file_output != NULL){
			len = fread(output, sizeof(char), STR_CHARS - 1 , file_output);
			if (len>0){
				output[len-1]='\000';
			}
			pclose(file_output);
		}
		
		/* en.status is set =0 (UNDEFINED) here and it is tested if it is !=0 before the registry update: the update is done only if en.status is !=0*/
		en.status=UNDEFINED;
		
		JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,jobid[k]);

		maxtok_l = strtoken(output, '\n', line);
	 
		for(i=0;i<maxtok_l;i++){

			if(line[i] && strstr(line[i],"Exit_status=")){	
				maxtok_t = strtoken(line[i], ' ', token);
                        	if((timestamp=calloc(STR_CHARS,1)) == 0){
                        	        sysfatal("can't malloc wn in PollDB: %r");
                        	}
                        	sprintf(timestamp,"%s %s",token[0],token[1]);
				tmstampepoch=str2epoch(timestamp,"A");
				exit_str=strdup(token[3]);
				free(exit_str);
				free(timestamp);
                        	for(j=0;j<maxtok_t;j++){
					free(token[j]);
                        	}
				maxtok_t = strtoken(exit_str, '=', token);
				en.udate=tmstampepoch;
                        	en.exitcode=atoi(token[1]);
				en.status=COMPLETED;
				JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
                        	for(j=0;j<maxtok_t;j++){
                                	free(token[j]);
                        	}
			}
			if(line[i] && strstr(line[i],"Job deleted")){	
				maxtok_t = strtoken(line[i], ' ', token);
                        	if((timestamp=calloc(STR_CHARS,1)) == 0){
                        	        sysfatal("can't malloc wn in PollDB: %r");
                        	}
                        	sprintf(timestamp,"%s %s",token[0],token[1]);
				tmstampepoch=str2epoch(timestamp,"A");
				exit_str=strdup(token[3]);
				free(exit_str);
				free(timestamp);
                        	for(j=0;j<maxtok_t;j++){
					free(token[j]);
                        	}
				en.udate=tmstampepoch;
				en.status=REMOVED;
			}
		}
		
		if(debug>1){
			fprintf(debuglogfile, "%s: registry update in FinalStateQuery for: jobid=%s exitcode=%d status=%d\n",argv0,en.batch_id,en.exitcode,en.status);
			fflush(debuglogfile);
		}
		
		if(en.status !=UNDEFINED){
			if ((ret=job_registry_update(rha, &en)) < 0){
				fprintf(stderr,"Append of record returns %d: ",ret);
				perror("");
			}
		}
		
		for(i=0;i<maxtok_l;i++){
			free(line[i]);
		}
	}
	for(k=0;k<maxtok_j;k++){
		free(jobid[k]);
	}
	free(line);
	free(token);
	free(jobid);
	free(output);
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
		
	if ((ret=job_registry_update(rha, &en)) < 0)
	{
		fprintf(stderr,"Append of record %d returns %d: ",i,ret);
		perror("");
	}
	return(0);
}
