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

  Copyright (c) 2006 Istituto Nazionale di Fisica Nucleare (INFN).
  All rights reserved.
  See http://grid.infn.it/grid/license.html for license details.
*/
#ifndef CONDOR_CREATE_LOG_H_INCLUDED
#define CONDOR_CREATE_LOG_H_INCLUDED

int create_accounting_log(int argc, char** argname, char** argvalue);

#endif /* defined CONDOR_CREATE_LOG_H_INCLUDED */

