#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

conffile=$lsf_confpath/lsf.conf
lsf_confdir=`cat $conffile|grep LSF_CONFDIR| awk -F"=" '{ print $2 }'`
[ -f ${lsf_confdir}/profile.lsf ] && . ${lsf_confdir}/profile.lsf

jnr=0
jc=0
for job in  $@ ; do
        jnr=$(($jnr+1))
done
for  job in  $@ ; do
        requested=`echo $job | sed 's/^.*\///'`
        cmdout=`${lsf_binpath}/bkill $requested 2>&1`
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
