#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

conffile=$lsf_confpath/lsf.conf
lsf_confdir=`cat $conffile|grep LSF_CONFDIR| awk -F"=" '{ print $2 }'`
[ -f ${lsf_confdir}/profile.lsf ] && . ${lsf_confdir}/profile.lsf

requested=`echo $1 | sed 's/^.*\///'`
${lsf_binpath}/bstop $requested >/dev/null 2>&1
