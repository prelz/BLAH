#!/bin/bash

# Initialize env
if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

if [ ! -z "$PBS_BIN_PATH" ]; then
    binpath=${PBS_BIN_PATH}/
else
    binpath=/usr/pbs/bin/
fi

requested=`echo $1 | sed 's/^.*\///'`
${binpath}qdel $requested >/dev/null 2>&1
