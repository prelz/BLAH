#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

if [ -z "$sge_root" ]; then sge_root="/usr/local/sge/pro"; fi
if [ -r "$sge_root/${sge_cell:-default}/common/settings.sh" ]
then
  . $sge_root/${sge_cell:-default}/common/settings.sh
fi

requested=`echo $1 | sed 's/^.*\///'`
requestedshort=`expr match "$requested" '\([0-9]*\)'`

if [ ! -z "$requestedshort" ]
then
    qhold $requestedshort > /dev/null 2>&1
else
    exit 1
fi
