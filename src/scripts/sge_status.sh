#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

usage_string="Usage: $0 [-w] [-n]"

#get worker node info
getwn=""

#get creamport
getcreamport=""

###############################################################
# Parse parameters
###############################################################

while getopts "wn" arg 
do
    case "$arg" in
    w) getwn="--getworkernodes" ;;
    n) getcreamport="yes" ;;
    
    -) break ;;
    ?) echo $usage_string
       exit 1 ;;
    esac
done

shift `expr $OPTIND - 1`

if [ "x$getcreamport" == "xyes" ]
then
    exec `dirname $0`/blah_job_registry_lkup -n
fi

if [ -z "$sge_root" ]; then sge_root="/usr/local/sge/pro"; fi
if [ -r "$sge_root/${sge_cell:-default}/common/settings.sh" ]
then
  . $sge_root/${sge_cell:-default}/common/settings.sh
fi

exec $sge_helper_path --status $getwn "$@"

exit 1
