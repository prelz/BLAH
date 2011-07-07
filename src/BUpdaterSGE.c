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


extern int bfunctions_poll_timeout;

int main(int argc, char *argv[]){
    
    FILE *fd;
    job_registry_entry *en;
    time_t now;
    time_t purge_time=0;
    char *constraint=NULL;
    char *constraint2=NULL;
    char *query=NULL;
    char *queryStates=NULL;
    char *q=NULL;
    char *q2=NULL;
    char *pidfile=NULL;
/*    char *final_string=NULL;*/
//     char *cp=NULL;
    char *tpath;
    
    int version=0;
    int first=TRUE;
    int tmptim;
    int finstr_len=0;
    int loop_interval=DEFAULT_LOOP_INTERVAL;
    
    int fsq_ret=0;
    
    int c;
    
    int confirm_time=0;
    
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

    ret = config_get("sge_binpath",cha);
    if (ret == NULL){
	do_log(debuglogfile, debug, 1, "%s: key sge_binpath not found\n",argv0);
    } else {
	sge_binpath=strdup(ret->value);
	if(sge_binpath == NULL){
	    sysfatal("strdup failed for sge_binpath in main: %r");
	}
    }

    ret = config_get("sge_rootpath",cha);
    if (ret == NULL){
	do_log(debuglogfile, debug, 1, "%s: key sge_rootpath not found\n",argv0);
    } else {
	sge_rootpath=strdup(ret->value);
	if(sge_rootpath == NULL){
	    sysfatal("strdup failed for sge_rootpath in main: %r");
	}
	
	tpath=make_message("%s",sge_rootpath);
	if (opendir(tpath)==NULL){
	    do_log(debuglogfile, debug, 1, "%s: dir %s does not exist or is not readable\n",argv0,tpath);
	    sysfatal("dir %s does not exist or is not readable: %r",tpath);
	}
	free(tpath);
    }

    ret = config_get("sge_cellname",cha);
    if (ret == NULL){
	do_log(debuglogfile, debug, 1, "%s: key sge_cellname not found\n",argv0);
    } else {
	sge_cellname=strdup(ret->value);
	if(sge_cellname == NULL){
	    sysfatal("strdup failed for sge_cellname in main: %r");
	}
    }

    ret = config_get("sge_helperpath",cha);
    if (ret == NULL){
	do_log(debuglogfile, debug, 1, "%s: key sge_helperpath not found\n",argv0);
    } else {
	sge_helperpath=strdup(ret->value);
	if(sge_helperpath == NULL){
	    sysfatal("strdup failed for sge_helperpath in main: %r");
	}
    }
    ret = config_get("sge_helperpath",cha);
    if (ret == NULL){
	if(debug){
	    fprintf(debuglogfile, "%s: key sge_helperpath not found\n",argv0);
	    fflush(debuglogfile);
	}
    } else {
	sge_helperpath=strdup(ret->value);
	if(sge_helperpath == NULL){
	    sysfatal("strdup failed for sge_helperpath in main: %r");
	}
    }

    ret = config_get("sge_rootpath",cha);
    if (ret == NULL){
	if(debug){
	    fprintf(debuglogfile, "%s: key sge_rootpath not found\n",argv0);
	    fflush(debuglogfile);
	}
    } else {
	sge_rootpath=strdup(ret->value);
	if(sge_rootpath == NULL){
	    sysfatal("strdup failed for sge_rootpath in main: %r");
	}
    }

    ret = config_get("sge_cellname",cha);
    if (ret == NULL){
	if(debug){
	    fprintf(debuglogfile, "%s: key sge_cellname not found\n",argv0);
	    fflush(debuglogfile);
	}
    } else {
	sge_cellname=strdup(ret->value);
	if(sge_cellname == NULL){
	    sysfatal("strdup failed for sge_cellname in main: %r");
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
	
	//IntStateQuery();
	fd = job_registry_open(rha, "r");
	if (fd == NULL)
	{
	    do_log(debuglogfile, debug, 1, "%s: Error opening job registry %s\n",argv0,registry_file);
	    fprintf(stderr,"%s: Error opening job registry %s :",argv0,registry_file);
	    perror("");
	    sleep(loop_interval);
	    continue;
	}
	if (job_registry_rdlock(rha, fd) < 0)
	{
	    do_log(debuglogfile, debug, 1, "%s: Error read locking job registry %s\n",argv0,registry_file);
	    fprintf(stderr,"%s: Error read locking job registry %s :",argv0,registry_file);
	    perror("");
	    sleep(loop_interval);
	    continue;
	}
	job_registry_firstrec(rha,fd);
	fseek(fd,0L,SEEK_SET);
	if((constraint=calloc(STR_CHARS,1)) == 0){
	    sysfatal("can't malloc constraint %r");
	}
	if((query=calloc(STR_CHARS,1)) == 0){
	    sysfatal("can't malloc query %r");
	}
	if((queryStates=calloc(STR_CHARS,1)) == 0){
	    sysfatal("can't malloc query %r");
	}
	if((constraint2=calloc(STR_CHARS,1)) == 0){
	    sysfatal("can't malloc query %r");
	}
	
	query[0]=' ';
	queryStates[0]=' ';
	first=TRUE;
	while ((en = job_registry_get_next(rha, fd)) != NULL)
	{
	    if(((now - en->mdate) > finalstate_query_interval) && en->status!=3 && en->status!=4)
	    {
		/* create the constraint that will be used in condor_history command in FinalStateQuery*/
		sprintf(constraint," %s",en->batch_id);
		if (en->status==0) sprintf(constraint2," u");
		if (en->status==1) sprintf(constraint2," q");
		if (en->status==2) sprintf(constraint2," r");
		if (en->status==5) sprintf(constraint2," h");
		q=realloc(query,strlen(query)+strlen(constraint)+1);
		q2=realloc(queryStates,strlen(queryStates)+strlen(constraint2)+1);
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
		if(q2 != NULL){
		    queryStates=q2;
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
		strcat(queryStates,constraint2);
		runfinal=TRUE;
	    }
	    /* Assign Status=4 and ExitStatus=-1 to all entries that after alldone_interval are still not in a final state(3 or 4) */
	    if((now - en->mdate > alldone_interval) && en->status!=3 && en->status!=4 && !runfinal)
	    {
		time_t now;
		now=time(0);
		AssignState(en->batch_id,"4" ,"-1","\0","\0",now);
		free(now);
	    }
	    free(en);
	}
	if(runfinal){
	    FinalStateQuery(query,queryStates);
	}
	free(constraint);
	free(constraint2);
	free(query);
	free(queryStates);
	//free(q);
	fclose(fd);
	if (runfinal){
	    runfinal=FALSE;
	    sleep (5);
	}else sleep (60);
    }

    job_registry_destroy(rha);
    return(0);
}


int FinalStateQuery(char *query,char *queryStates){

    char *command_string,*cmd,*qstatJob,*qstatStates,*qstatNodes;
    char *qHostname, *qFailed, *qExit;
    char line[STR_CHARS],query_err[strlen(query)],fail[6];
    char **saveptr1,**saveptr3,**saveptr_err,**list_query,**list_queryStates,**list_qstat,**list_states,**list_nodes;
    FILE *file_output,*file_output_err;
    int numQuery=0,numQstat=0,numStates=0,numQueryStates=0,numQueryNodes=0,j=0,k=0,l=0,cont=0,linesQstat=0;
    int doQacct;
    time_t now;

    if((command_string=calloc(NUM_CHARS+strlen(query),1)) == 0)
	sysfatal("can't malloc command_string %r");
    if ((qHostname = calloc (100,1))==0)
	sysfatal("can't malloc qstatJob %r");
    if ((qFailed = calloc (10,1))==0)
	sysfatal("can't malloc qstatJob %r");
    if ((qExit = calloc (10,1))==0)
	sysfatal("can't malloc qstatJob %r");
    
    numQuery=strtoken(query,' ',&list_query);
    numQueryStates=strtoken(queryStates,' ',&list_queryStates);
    if (numQuery!=numQueryStates) return 1;

    sprintf(command_string,"%s/qstat",sge_binpath);
    if (debug) do_log(debuglogfile, debug, 1, "+-+command_string:%s\n",command_string);

    file_output = popen(command_string,"r");
    if (file_output == NULL) return 0;
    while (fgets(line,sizeof(line), file_output) != NULL) linesQstat++;
    pclose(file_output);

    if ((qstatJob = calloc (linesQstat*STR_CHARS,1))==0)
	sysfatal("can't malloc qstatJob %r");
    if ((qstatNodes=calloc(linesQstat*STR_CHARS,1)) == 0)
	sysfatal("can't malloc qstatNodes %r");
    if ((qstatStates=calloc(linesQstat*STR_CHARS,1)) == 0)
	sysfatal("can't malloc qstatStates %r");

    sprintf(qstatNodes," \0");
    sprintf(qstatJob,"\0");
    sprintf(qstatStates,"\0");
    
    //load in qstatJob list of jobids from qstat command exec
    file_output = popen(command_string,"r");
    if (file_output == NULL) return 0;
    while (fgets(line,sizeof(line), file_output) != NULL){
	cont=strtoken(line, ' ', &saveptr1);
	if ((strcmp(saveptr1[0],"job-ID")!=0)&&(strncmp(saveptr1[0],"-",1)!=0)){
	    if (j>0) sprintf(qstatJob,"%s %s",qstatJob, saveptr1[0]);
	    else sprintf(qstatJob,"%s",saveptr1[0]);
	    if (j>0) sprintf(qstatStates,"%s %s",qstatStates, saveptr1[4]);
	    else sprintf(qstatStates,"%s",saveptr1[4]);
	    if (strlen(saveptr1[7])>3){
		cont=strtoken(saveptr1[7], '@', &saveptr3);
		if (j>0) sprintf(qstatNodes,"%s %s",qstatNodes, saveptr3[1]);
		else sprintf(qstatNodes,"%s",saveptr3[1]);
		saveptr3[0]='\0';
	    }else{
		if (j>0) sprintf(qstatNodes,"%s %s",qstatNodes, "x");
		else sprintf(qstatNodes,"%s","x");
	    }
	}
	j++;
	line[0]='\0';
	saveptr1[0]='\0';
    }
    pclose( file_output );

    numQstat=strtoken(qstatJob,' ',&list_qstat);
    numStates=strtoken(qstatStates,' ',&list_states);
    numQueryStates=strtoken(queryStates,' ',&list_queryStates);
    numQueryNodes=strtoken(qstatNodes,' ',&list_nodes);

    //compare job registry jobids with qstat list jobids, if a job is in job registry
    //and not in qstat job list, so it must be checked
    k=0;
    query_err[0]='\0';
    while ( k < numQuery ){
	for (l=0;l<numQstat;l++){
	    if (strcmp(list_query[k],list_qstat[l])==0){
		if (strcmp(list_queryStates[k],list_states[l])!=0){
		    now=time(0);
		    if (strcmp(list_states[l],"u")==0) AssignState(list_query[k],"0","0","","",make_message("%d",now));
		    if (strcmp(list_states[l],"q")==0)  AssignState(list_query[k],"1","0","","",make_message("%d",now));
		    if (strcmp(list_states[l],"r")==0) AssignState(list_query[k],"2","0","",list_nodes[l],make_message("%d",now));
		    if (strcmp(list_states[l],"h")==0) AssignState(list_query[k],"5","0","","",make_message("%d",now));
		}
		break;
	    }
	}
	if ((l==numQstat)||(numQstat==0)){ //not finded in qstat
	    sprintf(command_string,"%s/qacct -j %s",sge_binpath,list_query[k]);
	    if (debug) do_log(debuglogfile, debug, 1, "+-+line 542,command_string:%s\n",command_string);
	    file_output = popen(command_string,"r");
	    if (file_output == NULL) return 0;
	    //if a job number is here means that job was in query previously and
	    //if now it's not in query and not finished (NULL qstat) it was deleted 
	    //or it's on transition time
	    if (fgets( line,sizeof(line), file_output )==NULL){
		strcat(query_err,list_query[k]);
		strcat(query_err," ");
	    }
	    //there is no problem to lost first line with previous fgets, because 
	    //it's only a line of =============================================
	    while (fgets( line,sizeof(line), file_output )!=NULL){
		cont=strtoken(line, ' ', &saveptr1);
		if (strcmp(saveptr1[0],"hostname")==0) qHostname=strdup(saveptr1[1]);
		if (strcmp(saveptr1[0],"failed")==0) qFailed=strdup(saveptr1[1]);
		if (strcmp(saveptr1[0],"exit_status")==0) qExit=strdup(saveptr1[1]);
	    }
	    pclose( file_output );
	    now=time(0);
	    if (strcmp(qExit,"137")==0){
		AssignState(list_query[k],"3","3",qFailed,"",make_message("%d",now));
	    }else{
		AssignState(list_query[k],"4",qExit,qFailed,qHostname,make_message("%d",now));
	    }
	}
	k++;
    }//end while k<numQuery

    //now check acumulated error jobids to verify if they are an error or not
    if (strcmp(query_err,"\0")!=0){
	sleep(60);
	int cont_err=0;
	int n=0;
	cont_err=strtoken(query_err, ' ', &saveptr_err);
	while (n < cont_err){
	    if(saveptr_err[n]){
		cmd=strdup(saveptr_err[n]);
	    }else{
		if((cmd=calloc(STR_CHARS,1)) == 0){
		    sysfatal("can't malloc cmd in GetAndSend: %r");
		}
		cmd=strdup("\0");
	    }
	    sprintf(command_string,"%s/qacct -j %s",sge_binpath,cmd);
	    if (debug) do_log(debuglogfile, debug, 1, "+-+line 587, command_string:%s\n",command_string);
	    file_output_err = popen(command_string,"r");
	    if (file_output_err == NULL) return 0;
	    //if a job number is here means that job was in query previously and
	    //if now it's not in query and not finished (NULL qstat) it was deleted 
	    if (fgets( line,sizeof(line), file_output_err )==NULL){
		now=time(0);
		AssignState(cmd,"3","3","reason=3"," ",make_message("%d",now));
		pclose( file_output_err );
		sprintf(command_string,"\0");
		n++;
		continue;
	    }
	    //there is no problem to lost first line with previous fgets, because 
	    //it's only a line of =============================================
	    while (fgets( line,sizeof(line), file_output_err )!=NULL){
		cont=strtoken(line, ' ', &saveptr1);
		if (strcmp(saveptr1[0],"hostname")==0) qHostname=strdup(saveptr1[1]);
		if (strcmp(saveptr1[0],"failed")==0) qFailed=strdup(saveptr1[1]);
		if (strcmp(saveptr1[0],"exit_status")==0) qExit=strdup(saveptr1[1]);
	    }
	    now=time(0);
	    if (strcmp(qExit,"137")==0) AssignState(cmd,"3","3",qFailed,"",make_message("%d",now));
	    else AssignState(cmd,"4",qExit,qFailed,qHostname,make_message("%d",now));
	    pclose( file_output_err );
	    n++;
	}
// 	free(cmd);
    }
    
    free(command_string);
    free(qHostname);
    free(qFailed);
    free(qExit);
    free(qstatJob);
    free(qstatNodes);
    free(qstatStates);
    
    return 0;
}

int AssignState (char *element, char *status, char *exit, char *reason, char *wn, char *udate){
    char **id_element;
    job_registry_entry en;
    time_t now;
    char *string_now=NULL;
    int n=strtoken(element, '.', &id_element);
    
    if(id_element[0]){
	JOB_REGISTRY_ASSIGN_ENTRY(en.batch_id,id_element[0]);
	en.status=atoi(status);
	en.exitcode=atoi(exit);
	JOB_REGISTRY_ASSIGN_ENTRY(en.wn_addr,wn);
	JOB_REGISTRY_ASSIGN_ENTRY(en.exitreason,reason);
	now=time(0);
	string_now=make_message("%d",now);
	JOB_REGISTRY_ASSIGN_ENTRY(en.updater_info,string_now)
	en.udate=now;
	free(string_now);
    }else{
	if((element=calloc(STR_CHARS,1)) == 0){
	    sysfatal("can't malloc cmd in GetAndSend: %r");
	}
    }
    if ((ret=job_registry_update(rha, &en)) < 0){
	fprintf(stderr,"Update of record returns %d: \nJobId: &d", ret,en.batch_id);
	perror("");
    }else{
	if (en.status == REMOVED || en.status == COMPLETED){
	    job_registry_unlink_proxy(rha, &en);
	}
    }
    free(element);
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
    printf("Usage: BUpdaterSGE [OPTION...]\n");
    printf("  -o, --nodaemon     do not run as daemon\n");
    printf("  -v, --version      print version and exit\n");
    printf("\n");
    printf("Help options:\n");
    printf("  -?, --help         Show this help message\n");
    printf("  --usage            Display brief usage message\n");
    printf("  --test            Display a error message\n");
    exit(EXIT_SUCCESS);
}

int 
short_usage()
{
    printf("Usage: BUpdaterSGE [-ov?] [-o|--nodaemon] [-v|--version] [-?|--help] [--usage]\n");
    exit(EXIT_SUCCESS);
}
