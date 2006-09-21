/*
  File: test_condor_create_log.c
  Author : Giuseppe Fiorentino 
  Email: giuseppe.fiorentino@mi.infn.it
  
  Description:
  Command line tool for testing log-accounting functionality
  of libglite_ce_condor_logger library

  Copyright (c) 2006 Istituto Nazionale di Fisica Nucleare (INFN).
  All rights reserved.
  See http://grid.infn.it/grid/license.html for license details.
*/

#include <string.h>
#include "condor_create_log.h"

#define MAX_TEST_ARGS 100

void splitargs(char* argument, char* arg_name, char* arg_value);

int main (int argc, char** argv)
{
        int i=0;
	char *value[MAX_TEST_ARGS];
        char *name[MAX_TEST_ARGS];
	
        if ((argc > 1)&&(argc <= MAX_TEST_ARGS + 1 ))
	{	
		for (i=0;i<argc-1;i++)
		{
			name[i]=(char*)malloc(strlen(argv[i+1]));
			value[i]=(char*)malloc(strlen(argv[i+1]));
			splitargs(argv[i+1],name[i],value[i]);
		}
		for (i=0;i<argc-1;i++) printf("%s=%s\n", name[i], value[i]);
		create_accounting_log(argc-1, name, value);
	}
}

void splitargs(char* argument, char* arg_name, char* arg_value)
{
        int slen,i,j;
        slen=i=j=0;
        slen=strlen(argument);
        while(i<slen)
        {
                if (argument[i]=='=')
                {
                        memcpy(arg_name,argument,i);
                        arg_name[i]=0;
			j=++i;
                }else
                if (i == (slen -1))
                {
                        memcpy(arg_value,&argument[j],slen -j);
                        arg_value[slen-j]=0;
			break;
                }else   i++;
        }
}

