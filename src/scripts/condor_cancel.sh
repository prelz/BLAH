#!/bin/bash
#
# 	File:     condor_cancel.sh
# 	Author:   Giuseppe Fiorentino (giuseppe.fiorentino@mi.infn.it)
# 	Email:    giuseppe.fiorentino@mi.infn.it
#
# 	Revision history:
# 	08-Aug-2006: Original release
#
# 	Description:
#   	Cancellation script for Condor, to be invoked by blahpd server.
#   	Usage:
#     		condor_cancel.sh <jobid>
#
#  	Copyright (c) 2006 Istituto Nazionale di Fisica Nucleare (INFN).
#  	All rights reserved.
#  	See http://grid.infn.it/grid/license.html for license details.
#

jnr=0
jc=0
for job in  $@ ; do
        jnr=$(($jnr+1))
done
for  job in  $@ ; do
        requested=`echo $job | sed 's/^.*\///'`
        condor_rm $requested >/dev/null 2>&1
        if [ "$?" == "0" ] ; then
                if [ "$jnr" == "1" ]; then
                        echo " 0 No\\ error"
                else
                        echo .$jc" 0 No\\ error"
                fi
        else
                if [ "$jnr" == "1" ]; then
                        echo "1 Error"
                else
                        echo .$jc" 1 Error"
                fi
        fi
        jc=$(($jc+1))
done

