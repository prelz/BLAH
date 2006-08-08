#!/bin/bash
#
# 	File:     condor_hold.sh
# 	Author:   Giuseppe Fiorentino (giuseppe.fiorentino@mi.infn.it)
# 	Email:    giuseppe.fiorentino@mi.infn.it
#
# 	Revision history:
# 	08-Aug-2006: Original release
#
# 	Description:
#   	Hold script for Condor, to be invoked by blahpd server.
#   	Usage:
#          condor_hold.sh <jobid>

requested=`echo $1 | sed 's/^.*\///'`
requestedshort=`expr match "$requested" '\([0-9]*\)'`

result=`condor_q $requestedshort | awk '{ print $6}'`
status=`echo $result | awk '{ print $3}'`
if [ "$?" == "0" ] ; then
	if [ "$status" == "H" ] ; then
		exit 1
	fi
	condor_hold $requested  2>&1
fi
exit $?
	

