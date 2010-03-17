/*
  File: condor_create_log.h
  Author : Giuseppe Fiorentino
  Email: giuseppe.fiorentino@mi.infn.it

  Description:
  Exported function int create_accounting_log(int argc, char** argname, char** argvalue)
  for accounting functionality
  Fields to be logged:     
  x509userproxy, JobID (mandatory)
  edg_jobid,ceid,Queue (can be missing)
  argname value is case-insensitive (ceid same as CeID)

  Return values :
	0 success 
	1 failure

  Copyright (c) Members of the EGEE Collaboration. 2007-2010. 

    See http://www.eu-egee.org/partners/ for details on the copyright
    holders.  
  
    Licensed under the Apache License, Version 2.0 (the "License"); 
    you may not use this file except in compliance with the License. 
    You may obtain a copy of the License at 
  
        http://www.apache.org/licenses/LICENSE-2.0 
  
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
    See the License for the specific language governing permissions and 
    limitations under the License.
*/
#ifndef CONDOR_CREATE_LOG_H_INCLUDED
#define CONDOR_CREATE_LOG_H_INCLUDED

int create_accounting_log(int argc, char** argname, char** argvalue);

#endif /* defined CONDOR_CREATE_LOG_H_INCLUDED */

