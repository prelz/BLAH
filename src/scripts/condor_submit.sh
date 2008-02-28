#!/bin/bash
#
# 	File:     condor_submit.sh
# 	Author:   Giuseppe Fiorentino (giuseppe.fiorentino@mi.infn.it)
# 	Email:    giuseppe.fiorentino@mi.infn.it
#
# 	Revision history:
# 	08-Aug-2006: Original release
#
# 	Description:
#   	Submission script for Condor, to be invoked by blahpd server.
#   	Usage:
#  	condor_submit.sh -c <command> [-i <stdin>] [-o <stdout>] [-e <stderr>] [-w working dir] [-- command's arguments]
#
#  	Copyright (c) 2006 Istituto Nazionale di Fisica Nucleare (INFN).
#  	All rights reserved.
#  	See http://grid.infn.it/grid/license.html for license details.
#

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

usage_string="Usage: $0 -c <command> [-i <stdin>] [-o <stdout>] [-e <stderr>] [-v <environment>] [-s <yes | no>] [-- command_arguments]"

workdir=$PWD

proxy_dir=~/.blah_jobproxy_dir

###############################################################
# Parse parameters
###############################################################
original_args=$@
# Note: -s (stage command) s ignored as it is not relevant for Condor.

while getopts "i:o:e:j:v:V:c:w:x:q:s:T:I:O:R:" arg 
do
    case "$arg" in
    i) stdin="$OPTARG" ;;
    o) stdout="$OPTARG" ;;
    e) stderr="$OPTARG" ;;
    j) creamjobid="$OPTARG" ;;
    v) envir="$OPTARG";;
    V) environment="$OPTARG";;
    c) command="$OPTARG" ;;
    w) workdir="$OPTARG";;
    x) proxy_file="$OPTARG" ;;
    q) queue="$OPTARG";;
    s) stgcmd="$OPTARG" ;;
    T) temp_dir="$OPTARG" ;;
    I) inputflstring="$OPTARG" ;;
    O) outputflstring="$OPTARG" ;;
    R) remaps="$OPTARG" ;;
    -) break ;;
    ?) echo $usage_string
       exit 1 ;;
    esac
done

if [ -z "$temp_dir"  ] ; then
      curdir=`pwd`
      temp_dir="$curdir"
fi

shift `expr $OPTIND - 1`
arguments=$*


# Command is mandatory
if [ "x$command" == "x" ]
then
    echo $usage_string
    exit 1
fi

# Move into the IWD so we don't clutter the current working directory.
curdir=`pwd`
if [ "x$workdir" != "x" ]; then
    cd $workdir
    if [ $? -ne 0 ]; then
	echo "Failed to CD to Initial Working Directory." >&2
	echo Error # for the sake of waiting fgets in blahpd
	exit 1
    fi
fi

##############################################################
# Create submit file
###############################################################

submit_file=`mktemp -q $temp_dir/blahXXXXXX`
if [ $? -ne 0 ]; then
    echo "mktemp failed" >&2
    echo Error
    exit 1
fi

#  Remove any preexisting submit file
if [ -f $submit_file ] ; then
	rm -f $submit_file
fi

if [ ! -z "$inputflstring" ] ; then
    i=0
    for file in `cat $inputflstring`; do
	input_files[$i]=$file
	i=$((i+1))
    done
fi

if [ ! -z "$outputflstring" ] ; then
    i=0
    for file in `cat $outputflstring`; do
	output_files[$i]=$file
	i=$((i+1))
    done
fi

if [ ! -z "$remaps" ] ; then
    i=0
    for file in `cat $remaps`; do
	remap_files[$i]=$file
	i=$((i+1))
    done
fi

if [ ${#input_files[@]} -gt 0 ] ; then
    transfer_input_files="transfer_input_files=${input_files[0]}"
    for ((i=1; i < ${#input_files[@]}; i=$((i+1)))) ; do
	transfer_input_files="$transfer_input_files,${input_files[$i]}"
    done
fi

if [ ${#output_files[@]} -gt 0 ] ; then
    transfer_output_files="transfer_output_files=${output_files[0]}"
    for ((i=1; i < ${#output_files[@]}; i=$((i+1)))) ; do
	transfer_output_files="$transfer_output_files,${output_files[$i]}"
    done
fi

if [ ${#remap_files[@]} -gt 0 ] ; then
    if [ ! -z "${remap_files[0]}" ] ; then
	map=${remap_files[0]}
    else
	map=${output_files[0]}
    fi
    transfer_output_remaps="transfer_output_remaps=\"${output_files[0]}=$map"
    for ((i=1; i < ${#remap_files[@]}; i=$((i+1)))) ; do
	if [ ! -z "${remap_files[0]}" ] ; then
	    map=${remap_files[$i]}
	else
	    map=${output_files$i]}
	fi
	transfer_output_remaps="$transfer_output_remaps;${output_files[$i]}=$map"
    done
    transfer_output_remaps="$transfer_output_remaps\""
fi

# Convert input environment (old Condor or shell format as dictated by 
# input args):

submit_file_environment="#"

if [ "x$environment" != "x" ] ; then
# Input format is suitable for bourne shell style assignment. Convert to
# old condor format (no double quotes in submit file).
# FIXME: probably it's better to convert everything into the 'new' Condor
# environment format.
    eval "env_array=($environment)"
    submit_file_environment=""
    for  env_var in "${env_array[@]}"; do
        if [ "x$submit_file_environment" == "x" ] ; then
            submit_file_environment="environment = "
        else
            submit_file_environment="$submit_file_environment;"
        fi
        submit_file_environment="${submit_file_environment}${env_var}"
    done
else
    if [ "x$envir" != "x" ] ; then
# Old Condor format (no double quotes in submit file)
        submit_file_environment="environment = $envir"
    fi
fi

### This appears to only be necessary if Condor is passing arguments
### with the "new_esc_format"
# # NOTE: The arguments we are given are specially escaped for a shell,
# # so to get them back into Condor format we need to remove all the
# # extra quotes. We do this by replacing '" "' with ' ' and stripping
# # the leading and trailing "s.
# arguments="$(echo $arguments | sed -e 's/\" \"/ /g')"
# arguments=${arguments:1:$((${#arguments}-2))}

cat > $submit_file << EOF
universe = vanilla
executable = $command
x509userproxy = $proxy_file
# We insist on new style quoting in Condor
arguments = $arguments
input = $stdin
output = $stdout
error = $stderr
$transfer_input_files
$transfer_output_files
$transfer_output_remaps
when_to_transfer_output = on_exit
should_transfer_files = yes
notification = error
$submit_file_environment
# Hang around for 1 day (86400 seconds) ?
# Hang around for 30 minutes (1800 seconds) ?
leave_in_queue = JobStatus == 4 && (CompletionDate =?= UNDEFINED || CompletionDate == 0 || ((CurrentTime - CompletionDate) < 1800))

queue 1
EOF

###############################################################
# Perform submission
###############################################################

# Actual submission to condor to allow job enter the queue. The queue
# variable may be two parameters, separated by a space. If it is the
# first param is the name of the queue and the second is the name of
# the pool where the queue exists, i.e. a Collector's name.

echo $queue | grep "/" >&/dev/null
# If there is a "/" we need to split out the pool and queue
if [ "$?" == "0" ]; then
    pool=${queue#*/}
    queue=${queue%/*}
fi

if [ -z "$queue" ]; then
    target=""
else
    if [ -z "$pool" ]; then
	target="-name $queue"
    else
	target="-pool $pool -name $queue"
    fi
fi

now=`date +%s`

full_result=$($condor_binpath/condor_submit $target $submit_file)
return_code=$?

if [ "$return_code" == "0" ] ; then
    jobID=`echo $full_result | awk '{print $8}' | tr -d '.'`
    blahp_jobID="condor/$jobID/$queue/$pool"

    if [ "x$job_registry" != "x" ]; then
      `dirname $0`/blah_job_registry_add "$blahp_jobID" "$jobID" 1 $now-1 "$creamjobid"
    fi

    echo "BLAHP_JOBID_PREFIX$blahp_jobID"
else
    echo "Failed to submit"
    echo Error
fi

# Clean temporary files -- There only temp file is the one we submit
#rm -f $submit_file

# Create a softlink to proxy file for proxy renewal - local renewal 
# of limited proxy only.

if [ -r "$proxy_file" -a -f "$proxy_file" ] ; then
    [ -d "$proxy_dir" ] || mkdir $proxy_dir
    ln -s $proxy_file $proxy_dir/$jobID.proxy.norenew
fi

exit $return_code
