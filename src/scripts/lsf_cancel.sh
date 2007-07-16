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
        ${lsf_binpath}/bkill $requested >/dev/null 2>&1
        if [ "$?" == "0" ] ; then
                if [ "$jnr" == "1" ]; then
                        echo " 0 No\\ error"
                else
                        echo .$jc" 0 No\\ error"
                fi
        else
                if [ "$jnr" == "1" ]; then
                        echo " 1 Error"
                else
                        echo .$jc" 1 Error"
                fi
        fi
        jc=$(($jc+1))
done
