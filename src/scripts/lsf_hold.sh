#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

requested=`echo $1 | sed 's/^.*\///'`
${lsf_binpath}/bstop $requested >/dev/null 2>&1
