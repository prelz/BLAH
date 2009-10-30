#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

jnr=0
jc=0
for job in  $@ ; do
        jnr=$(($jnr+1))
done
for  job in  $@ ; do
        requested=`echo $job | sed 's/^.*\///'`
        cmdout=`${pbs_binpath}/qdel $requested 2>&1`
        retcode=$?
        if [ "$retcode" == "0" ] ; then
                if [ "$jnr" == "1" ]; then
                        echo " 0 No\\ error"
                else
                        echo .$jc" 0 No\\ error"
                fi
        else
                escaped_cmdout=`echo $cmdout|sed "s/ /\\\\\ /g"`
                if [ "$jnr" == "1" ]; then
                        echo " $retcode $escaped_cmdout"
                else
                        echo .$jc" $retcode $escaped_cmdout"
                fi
        fi
        jc=$(($jc+1))
done

