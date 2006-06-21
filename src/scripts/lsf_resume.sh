#!/bin/bash

[ -f $GLITE_LOCATION/etc/blah.config ] && . $GLITE_LOCATION/etc/blah.config

requested=`echo $1 | sed 's/^.*\///'`
${lsf_binpath}/bresume $requested >/dev/null 2>&1
