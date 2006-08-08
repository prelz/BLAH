#!/bin/bash
#
# 	File:     condor_resume.sh
# 	Author:   Giuseppe Fiorentino (giuseppe.fiorentino@mi.infn.it)
# 	Email:    giuseppe.fiorentino@mi.infn.it
#
# 	Revision history:
# 	08-Aug-2006: Original release
#
# 	Description:
#   	Resume script for Condor, to be invoked by blahpd server.
#   	Usage:
#     	   condor_resume.sh <jobid>

requested=`echo $1 | sed 's/^.*\///'`
condor_release $requested >/dev/null 2>&1
