#!/bin/bash

# Initialize env
if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

if [ ! -z "$LSF_BIN_PATH" ]; then
    binpath=${LSF_BIN_PATH}/
else
    binpath=/usr/local/lsf/bin/
fi

requested=`echo $1 | sed 's/^.*\///'`
${binpath}bkill $requested >/dev/null 2>&1
