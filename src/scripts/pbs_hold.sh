#!/bin/bash

# Initialize env
if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

requested=`echo $1 | sed 's/^.*\///'`

#currently only holding idle jobs is supported
if [ "$2" ==  "1" ]; then
	${PBS_BIN_PATH:-/usr/bin}/qhold $requested >/dev/null 2>&1
else
	exit 1
fi
