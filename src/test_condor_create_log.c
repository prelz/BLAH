/*
# File: test_condor_create_log.c
# Author : Giuseppe Fiorentino 
# Email: giuseppe.fiorentino@mi.infn.it
# 
# Description:
# Command line tool for testing log-accounting functionality
# of libglite_ce_condor_logger library
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

*/

#include <stdio.h>
#include <stdlib.h>
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

