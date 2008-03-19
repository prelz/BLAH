#include "BUpdaterLSF.h"

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
			fprintf(debuglogfile, "%s: key lsf_binpath not found\n",argv0);
			fflush(debuglogfile);
		}
        } else {
                lsf_binpath=strdup(ret->value);
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

			/* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4) */
			if((now-en->mdate>alldone_interval) && en->status!=3 && en->status!=4)
			{
				AssignFinalState(en->batch_id);	
			}
			
			if((now-en->mdate>finalstate_query_interval) && en->status!=3 && en->status!=4)
			{
				/* create the constraint that will be used in condor_history command in FinalStateQuery*/
				if(!first) strcat(query," ||");	
				if(first) first=FALSE;
				sprintf(constraint," ClusterId==%s",en->batch_id);
				
				q=realloc(query,strlen(query)+strlen(constraint)+4);
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
			free(en);
		}
		
		if(runfinal){
			FinalStateQuery(query);
			runfinal=FALSE;
		}
		free(constraint);		
		free(query);
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

/*
 bjobs -u all -a -l 
 In the first 2 lines:
Job <939297>, Job Name <cre02_199385905>, User <infngrid010>, Project <default>
                     , Status <EXIT>, Queue <creamtest2>, Command <#!/bin/bash;
there are:
batch_id (Job <939297>)
eventually  blah_id (Job Name <cre02_199385905>)
status (Status <EXIT>)

In line:
Tue Mar 18 13:30:36: Started on <cream-wn-024>, Execution Home </home/infngrid0

there are:
udate for the state running(Tue Mar 18 13:30:36)
wn_addr (Started on <cream-wn-024>)

in line:
Tue Mar 18 13:47:32: Exited with exit code 2. The CPU time used is 1.8 seconds.
or
Tue Mar 18 12:48:25: Done successfully. The CPU time used is 2.1 seconds.

there are:
udate for the final state (Tue Mar 18 13:47:32):
exitcode (=0 if Done successfully) or (from Exited with exit code 2)
*/

	char *output;
        FILE *file_output;
	int len;
	char **line;
	char **token;
	int maxtok_l=0,maxtok_t=0,i,j;
	job_registry_entry en;
	int ret;

        if((output=calloc(STR_CHARS,1)) == 0){
                printf("can't malloc output\n");
        }
	if((line=calloc(100 * sizeof *line,1)) == 0){
		sysfatal("can't malloc line %r");
	}
	if((token=calloc(100 * sizeof *token,1)) == 0){
		sysfatal("can't malloc token %r");
	}
	if((command_string=calloc(STR_CHARS,1)) == 0){
		sysfatal("can't malloc command_string %r");
	}
	
	sprintf(command_string,"%s/bjobs -u all -a -l",lsf_binpath);
	file_output = popen(command_string,"r");

        if (file_output != NULL){
                len = fread(output, sizeof(char), STR_CHARS - 1 , file_output);
                if (len>0){
                        output[len-1]='\000';
                }
        }
        pclose(file_output);
	
	maxtok_l = strtoken(output, '\n', line);
	for(i=0;i<maxtok_l;i++){
		maxtok_t = strtoken(line[i], ' ', token);
		
		JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,token[0]);
		en.status=atoi(token[2]);
		en.exitcode=atoi(token[4]);
		en.udate=atoi(token[5]);
		JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
		JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
				
		if ((ret=job_registry_update(rha, &en)) < 0)
		{
			fprintf(stderr,"Append of record returns %d: ",ret);
			perror("");
		}
		
		for(j=0;j<maxtok_t;j++){
			free(token[j]);
		}
	}

	for(i=0;i<maxtok_l;i++){
		free(line[i]);
	}
	free(line);
	free(token);
	free(output);
	free(command_string);
	return(0);
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

 	char *output;
        FILE *file_output;
	int len;
	char **line;
	char **token;
	int maxtok_l=0,maxtok_t=0,i,j;
	job_registry_entry en;
	int ret;

        if((output=calloc(STR_CHARS,1)) == 0){
                printf("can't malloc output\n");
        }
	if((line=calloc(100 * sizeof *line,1)) == 0){
		sysfatal("can't malloc line %r");
	}
	if((token=calloc(100 * sizeof *token,1)) == 0){
		sysfatal("can't malloc token %r");
	}
	if((command_string=calloc(NUM_CHARS+strlen(query),1)) == 0){
		sysfatal("can't malloc command_string %r");
	}

	sprintf(command_string,"%s/bhist -u all -a -l",lsf_binpath);
	file_output = popen(command_string,"r");

        if (file_output != NULL){
                len = fread(output, sizeof(char), STR_CHARS - 1 , file_output);
                if (len>0){
                        output[len-1]='\000';
                }
        }
        pclose(file_output);
	
	maxtok_l = strtoken(output, '\n', line);   
	for(i=0;i<maxtok_l;i++){
		maxtok_t = strtoken(line[i], ' ', token);
		
		JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,token[0]);
		en.status=atoi(token[2]);
		en.exitcode=atoi(token[4]);
		en.udate=atoi(token[5]);
                JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,"\0");
                JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,"\0");
		
		if ((ret=job_registry_update(rha, &en)) < 0)
		{
			fprintf(stderr,"Append of record %d returns %d: ",i,ret);
			perror("");
		}
		
		for(j=0;j<maxtok_t;j++){
			free(token[j]);
		}
	}
	for(i=0;i<maxtok_l;i++){
		free(line[i]);
	}
	free(line);
	free(token);
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
