#!/bin/bash

# Initialize env
if  [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

if  [ -f ~/.login ]; then
 . ~/.login
fi

requested=`echo $1 | sed 's/^.*\///'`
${PBS_BIN_PATH:-/usr/bin}/qrls $requested >/dev/null 2>&1
