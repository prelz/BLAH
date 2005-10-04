#!/bin/bash

# Initialize env
if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

requested=`echo $1 | sed 's/^.*\///'`
${LSF_BIN_PATH:-/usr/local/lsf/bin}/bresume $requested >/dev/null 2>&1
