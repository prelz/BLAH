#include "job_registry.h"
#include "Bfunctions.h"
#include "config.h"

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

int IntStateQuery();
int FinalStateQuery(char *query);
int AssignFinalState(char *batchid);

int runfinal=FALSE;
char *command_string;
char *condor_binpath;
char *registry_file;
int purge_interval=864000;
int finalstate_query_interval=30;
int alldone_interval=600;
int debug=0;
int nodmn=0;

FILE *debuglogfile;
char *debuglogname;

job_registry_handle *rha;
config_handle *cha;
config_entry *ret;
char *progname="BUpdaterCondor";

