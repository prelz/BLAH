#!/bin/bash

#  	File:     condor_status.sh
#
#  	Author:   Giuseppe Fiorentino
#  	Email:   giuseppe.fiorentino@mi.infn.it
#
#  	Revision history:
#    	08-Aug-2006: Original release
#
#  	Description:
#    		Returns a classad describing the status of a Condor job
#
#	 Usage:
#	     condor_status.sh <jobid>
#
#  	Copyright (c) 2006 Istituto Nazionale di Fisica Nucleare (INFN).
#  	All rights reserved.
#  	See http://grid.infn.it/grid/license.html for license details.
#

usage_string="Usage: $0 [-w] [-n]"

###############################################################
# Parse parameters
###############################################################

# Options not implemented yet
while getopts "wn" arg 
do
    case "$arg" in
    w) getwn="yes" ;;
    n) getcreamport="yes" ;;
    
    -) break ;;
    ?) echo $usage_string
       exit 1 ;;
    esac
done

shift `expr $OPTIND - 1`

###################################################################
pars=$*

#          States of the job.
#          U = unexpanded (never been run), H = on hold, R = running, 
#          I = idle (waiting for a  machine  to execute on), C = completed,
#          and X = removed.

for  reqfull in $pars ; do
        requested=${reqfull:7}
	reqjob=`echo $requested | sed -e 's/^.*\///'`
        result=`condor_q $reqjob | awk '{ print $6}'`
        result=`echo $result | awk '{ print $3}'`
        if [ "$?" == "0" ] ; then
               case "x$result" in
    			xI) status="1" ;;
    			xR) status="2" ;;
                       	xX) status="3" ;;
                        xC) status="4" ;;
			xH) status="5" ;;
                        x)  status="undef" ;;
    		esac
		#May be necessary to seek in terminated jobs 
                if [ "x$status" == "xundef" ] ; then
		        result=`condor_history $reqjob | awk '{ print $6}'`
  			result=`echo $result | awk '{ print $2}'`
                        case "x$result" in
                        	xC) status="4" ;;
                        	xX) status="3" ;;
                	esac
                fi
		if  [ "x$status" == "xundef" ] ; then
			echo 1
                else
                	echo "0[BatchjobId=\"$reqjob\";JobStatus=\"$status\";]"
		fi
	else
		echo 1
	fi 
done
exit 0
