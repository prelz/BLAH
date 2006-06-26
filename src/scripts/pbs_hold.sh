#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

requested=`echo $1 | sed 's/^.*\///'`
requestedshort=`expr match "$requested" '\([0-9]*\)'`
result=`${pbs_binpath}/qstat | awk -v jobid="$requestedshort" '
$0 ~ jobid {
	print $5
}
'`
#currently only holding idle or waiting jobs is supported
if [ "$2" ==  "1" ] ; then
	${pbs_binpath}/qhold $requested
else
	if [ "$result" == "W" ] ; then
		${pbs_binpath}/qhold $requested
	else
		exit 1
	fi
fi
