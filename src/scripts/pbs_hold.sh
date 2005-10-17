#!/bin/bash

# Initialize env
if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

requested=`echo $1 | sed 's/^.*\///'`
requestedshort=`expr match "$requested" '\([0-9]*\)'`
result=`${PBS_BIN_PATH:-/usr/bin}/qstat | awk -v jobid="$requestedshort" '
$0 ~ jobid {
	print $5
}
'`
#currently only holding idle or waiting jobs is supported
if [ "$2" ==  "1" ] ; then
	${PBS_BIN_PATH:-/usr/bin}/qhold $requested
else
	if [ "$result" == "W" ] ; then
		${PBS_BIN_PATH:-/usr/bin}/qhold $requested
	else
		exit 1
	fi
fi