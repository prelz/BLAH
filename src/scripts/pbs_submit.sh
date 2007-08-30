#!/bin/bash
#
# File:     pbs_submit.sh
# Author:   David Rebatto (david.rebatto@mi.infn.it)
#
# Revision history:
#     8-Apr-2004: Original release
#    28-Apr-2004: Patched to handle arguments with spaces within (F. Prelz)
#                 -d debug option added (print the wrapper to stderr without submitting)
#    10-May-2004: Patched to handle environment with spaces, commas and equals
#    13-May-2004: Added cleanup of temporary file when successfully submitted
#    18-May-2004: Search job by name in log file (instead of searching by jobid)
#     8-Jul-2004: Try a chmod u+x on the file shipped as executable
#                 -w option added (cd into submission directory)
#    20-Sep-2004: -q option added (queue selection)
#    29-Sep-2004: -g option added (gianduiotto selection) and job_ID=job_ID_log
#    13-Jan-2005: -n option added (MPI job selection)
#     9-Mar-2005: Dgas(gianduia) removed. Proxy renewal stuff added (-r -p -l flags)
#     3-May-2005: Added support for Blah Log Parser daemon (using the pbs_BLParser flag)
# 
#
# Description:
#   Submission script for PBS, to be invoked by blahpd server.
#   Usage:
#     pbs_submit.sh -c <command> [-i <stdin>] [-o <stdout>] [-e <stderr>] [-w working dir] [-- command's arguments]
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
#

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

usage_string="Usage: $0 -c <command> [-i <stdin>] [-o <stdout>] [-e <stderr>] [-x <x509userproxy>] [-v <environment>] [-s <yes | no>] [-- command_arguments]"

logpath=${pbs_spoolpath}/server_logs

stgcmd="yes"
stgproxy="yes"

proxyrenewald="${GLITE_LOCATION:-/opt/glite}/bin/BPRserver"

#default is to stage proxy renewal daemon 
proxyrenew="yes"

if [ ! -r "$proxyrenewald" ]
then
  unset proxyrenew
fi

proxy_dir=~/.blah_jobproxy_dir

workdir=$PWD

srvfound=""

#default values for polling interval and min proxy lifetime
prnpoll=30
prnlifetime=0

BLClient="${GLITE_LOCATION:-/opt/glite}/bin/BLClient"

###############################################################
# Parse parameters
###############################################################
original_args=$@
while getopts "i:o:e:c:s:v:V:dw:q:n:rp:l:x:j:T:I:O:R:C:" arg 
do
    case "$arg" in
    i) stdin="$OPTARG" ;;
    o) stdout="$OPTARG" ;;
    e) stderr="$OPTARG" ;;
    v) envir="$OPTARG";;
    V) environment="$OPTARG";;
    c) the_command="$OPTARG" ;;
    s) stgcmd="$OPTARG" ;;
    d) debug="yes" ;;
    w) workdir="$OPTARG";;
    q) queue="$OPTARG";;
    n) mpinodes="$OPTARG";;
    r) proxyrenew="yes" ;;
    p) prnpoll="$OPTARG" ;;
    l) prnlifetime="$OPTARG" ;;
    x) proxy_string="$OPTARG" ;;
    j) creamjobid="$OPTARG" ;;
    T) temp_dir="$OPTARG" ;;
    I) inputflstring="$OPTARG" ;;
    O) outputflstring="$OPTARG" ;;
    R) outputflstringremap="$OPTARG" ;;
    C) req_file="$OPTARG";;
    -) break ;;
    ?) echo $usage_string
       exit 1 ;;
    esac
done

# Command is mandatory
if [ "x$the_command" == "x" ]
then
    echo $usage_string
    exit 1
fi

shift `expr $OPTIND - 1`
arguments=$*

if [ "x$pbs_nologaccess" != "xyes" -a "x$pbs_nochecksubmission" != "xyes" ]; then

#Try different logparser
 if [ ! -z $pbs_num_BLParser ] ; then
  for i in `seq 1 $pbs_num_BLParser` ; do
   s=`echo pbs_BLPserver${i}`
   p=`echo pbs_BLPport${i}`
   eval tsrv=\$$s
   eval tport=\$$p
   testres=`echo "TEST/"|$BLClient -a $tsrv -p $tport`
   if [ "x$testres" == "xYPBS" ] ; then
    pbs_BLPserver=$tsrv
    pbs_BLPport=$tport
    srvfound=1
    break
   fi
  done
  if [ -z $srvfound ] ; then
   echo "1ERROR: not able to talk with no logparser listed"
   exit 0
  fi
 fi
fi

###############################################################
# Create wrapper script
###############################################################

curdir=`pwd`
if [ -z "$temp_dir"  ] ; then
    temp_dir="$curdir"
else
    if [ ! -e $temp_dir ] ; then
        mkdir -p $temp_dir
    fi
    if [ ! -d $temp_dir -o ! -w $temp_dir ] ; then
        echo "1ERROR: unable to create or write to $temp_dir"
        exit 0
    fi
fi


# Get a suitable name for temp file
if [ "x$debug" != "xyes" ]
then
    if [ ! -z "$creamjobid"  ] ; then
        tmp_name="cream_${creamjobid}"
        tmp_file="$temp_dir/$tmp_name"
    else
        rand=$RANDOM$RANDOM$RANDOM$RANDOM
        tmp_name=bl_${rand:0:12}
        tmp_file="$temp_dir/$tmp_name"
        `touch $tmp_file;chmod 600 $tmp_file`
    fi
    if [ $? -ne 0 ]; then
        echo Error
        exit 1
    fi
else
    # Just print to stderr if in debug
    tmp_file="/proc/$$/fd/2"
fi

# Create unique extension for filenames
uni_uid=`id -u`
uni_pid=$$
uni_time=`date +%s`
uni_ext=$uni_uid.$uni_pid.$uni_time

# Put executable into inputsandbox

if [ "x$stgcmd" == "xyes" ] ; then
    blahpd_inputsandbox="`basename $the_command`@`hostname -f`:$the_command"
    to_be_moved="$to_be_moved `basename $the_command`"
fi

# Put BPRserver into sandbox
if [ "x$proxyrenew" == "xyes" ] ; then
    if [ -r "$proxyrenewald" ] ; then
        remote_BPRserver=`basename $proxyrenewald`.$uni_ext
        if [ ! -z $blahpd_inputsandbox ]; then blahpd_inputsandbox="${blahpd_inputsandbox},"; fi
        blahpd_inputsandbox="${blahpd_inputsandbox}${remote_BPRserver}@`hostname -f`:$proxyrenewald"
        to_be_moved="$to_be_moved $remote_BPRserver"
    else
        unset proxyrenew
    fi
fi

# Setup proxy transfer
need_to_reset_proxy=no
proxy_remote_file=
if [ "x$stgproxy" == "xyes" ] ; then
    proxy_local_file=${workdir}"/"`basename "$proxy_string"`;
    [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] || proxy_local_file=$proxy_string
    [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] || proxy_local_file=/tmp/x509up_u`id -u`
    if [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] ; then
        if [ ! -z $blahpd_inputsandbox ]; then blahpd_inputsandbox="${blahpd_inputsandbox},"; fi
        proxy_remote_file=${tmp_name}.proxy
        blahpd_inputsandbox="${blahpd_inputsandbox}${proxy_remote_file}@`hostname -f`:${proxy_local_file}"
        to_be_moved="$to_be_moved ${proxy_remote_file}"
        need_to_reset_proxy=yes
    fi
fi


# Setup stdout & stderr
if [ ! -z "$stdin" ] ; then
    if [ -f "$stdin" ] ; then
        stdin_unique=`basename $stdin`.$uni_ext
        if [ ! -z $blahpd_inputsandbox ]; then blahpd_inputsandbox="${blahpd_inputsandbox},"; fi
        blahpd_inputsandbox="${blahpd_inputsandbox}${stdin_unique}@`hostname -f`:${stdin}"
        to_be_moved="$to_be_moved $stdin_unique"
        arguments="$arguments <\"$stdin_unique\""
    else
        arguments="$arguments <$stdin"
    fi
fi
if [ ! -z "$stdout" ] ; then
    if [ "${stdout:0:1}" != "/" ] ; then stdout=${workdir}/${stdout} ; fi
    arguments="$arguments >`basename $stdout`"
    if [ ! -z $blahpd_outputsandbox ]; then blahpd_outputsandbox="${blahpd_outputsandbox},"; fi
    blahpd_outputsandbox="${blahpd_outputsandbox}home_${tmp_name}/`basename $stdout`@`hostname -f`:$stdout"
fi
if [ ! -z "$stderr" ] ; then
    if [ "${stderr:0:1}" != "/" ] ; then stderr=${workdir}/${stderr} ; fi
    if [ "$stderr" == "$stdout" ]; then
        arguments="$arguments 2>&1"
    else
        arguments="$arguments 2>`basename $stderr`"
        if [ ! -z $blahpd_outputsandbox ]; then blahpd_outputsandbox="${blahpd_outputsandbox},"; fi
        blahpd_outputsandbox="${blahpd_outputsandbox}home_${tmp_name}/`basename $stderr`@`hostname -f`:$stderr"
    fi
fi


# Write wrapper preamble
cat > $tmp_file << end_of_preamble
#!/bin/bash
# PBS job wrapper generated by `basename $0`
# on `/bin/date`
#
# stgcmd = $stgcmd
# proxy_string = $proxy_string
# proxy_local_file = $proxy_local_file
#
# PBS directives:
#PBS -S /bin/bash
end_of_preamble

#local batch system-specific file output must be added to the submit file
if [ ! -z $req_file ] ; then
    echo \#\!/bin/sh >> ${req_file}-temp_req_script
    cat $req_file >> ${req_file}-temp_req_script
    echo "source ${GLITE_LOCATION:-/opt/glite}/bin/pbs_local_submit_attributes.sh" >> ${req_file}-temp_req_script
    chmod +x ${req_file}-temp_req_script
    ${req_file}-temp_req_script  >> $tmp_file 2> /dev/null
    rm -f ${req_file}-temp_req_script
    rm -f $req_file
fi

# Write PBS directives according to command line options
# handle queue overriding
[ -z "$queue" ] || grep -q "^#PBS -q" $tmp_file || echo "#PBS -q $queue" >> $tmp_file
[ -z "$mpinodes" ]             || echo "#PBS -l nodes=$mpinodes" >> $tmp_file
[ -z "$blahpd_inputsandbox" ]  || echo "#PBS -W stagein=$blahpd_inputsandbox" >> $tmp_file
[ -z "$blahpd_outputsandbox" ] || echo "#PBS -W stageout=$blahpd_outputsandbox" >> $tmp_file
echo "#PBS -m n"  >> $tmp_file
#Add files to transfer to execution node
#absolute paths
 if [ ! -z "$inputflstring" ] ; then
         exec 4<> "$inputflstring"
         while read xfile <&4 ; do
               if [ ! -z $xfile  ] ; then
                       xfilesandbox="./`basename ${xfile}`@`hostname -f`:${xfile}"
                       echo "#PBS -W stagein=$xfilesandbox" >> $tmp_file
               fi
         done
         exec 4<&-
       rm -f $inputflstring
 fi
xfile=
xfilesandbox=
#Add files to transfer from execution node
 if [ ! -z "$outputflstring" ] ; then
        exec 5<> "$outputflstring"
        if [ ! -z "$outputflstringremap" ] ; then
                exec 6<> "$outputflstringremap"
        fi
        while read xfile <&5 ; do
               if [ ! -z $xfile  ] ; then
                       if [ ! -z "$outputflstringremap" ] ; then
                                read xfileremap <&6
                       fi

                       xfilesandbox="${xfile}@`hostname -f`"
                       if [ ! -z $xfileremap ] ; then
                                if [ "${xfileremap:0:1}" != "/" ] ; then
                                        xfilesandbox="${xfilesandbox}:${workdir}/${xfileremap}"
                                else
                                        xfilesandbox="${xfilesandbox}:${xfileremap}"
                                fi
                        else
                                xfilesandbox="${xfilesandbox}:${workdir}/${xfile}"
                        fi
                       echo "#PBS -W stageout=$xfilesandbox" >> $tmp_file
               fi
         done
         exec 5<&-
         exec 6<&-
         rm -f $outputflstring
         if [ ! -z "$outputflstringremap" ] ; then
                rm -f $outputflstringremap
         fi
 fi

# Set the required environment variables (escape values with double quotes)
if [ "x$environment" != "x" ] ; then
        echo "" >> $tmp_file
        echo "# Setting the environment:" >> $tmp_file
	eval "env_array=($environment)"
        for  env_var in "${env_array[@]}"; do
                 echo export \"$env_var\" >> $tmp_file
        done
else
        if [ "x$envir" != "x" ] ; then
                echo "" >> $tmp_file
                echo "# Setting the environment:" >> $tmp_file
                echo "`echo ';'$envir | sed -e 's/;[^=]*;/;/g' -e 's/;[^=]*$//g' | sed -e 's/;\([^=]*\)=\([^;]*\)/;export \1=\"\2\"/g' | awk 'BEGIN { RS = ";" } ; { print $0 }'`" >> $tmp_file
        fi
fi

# Set the temporary home (including cd'ing into it)
echo "mkdir ~/home_$tmp_name">>$tmp_file
[ -z "$to_be_moved" ] || echo "mv $to_be_moved ~/home_$tmp_name &>/dev/null">>$tmp_file
echo "export HOME=~/home_$tmp_name">>$tmp_file
echo "cd">>$tmp_file

# Set the path to the user proxy
if [ "x$need_to_reset_proxy" == "xyes" ] ; then
    echo "# Resetting proxy to local position" >> $tmp_file
    echo "export X509_USER_PROXY=\`pwd\`/${proxy_remote_file}" >> $tmp_file
fi

# Add the command (with full path if not staged)
echo "" >> $tmp_file
echo "# Command to execute:" >> $tmp_file
if [ "x$stgcmd" == "xyes" ] 
then
    the_command="./`basename $the_command`"
    echo "if [ ! -x $the_command ]; then chmod u+x $the_command; fi" >> $tmp_file
fi
echo "$the_command $arguments &" >> $tmp_file

echo "job_pid=\$!" >> $tmp_file

if [ ! -z $proxyrenew ]
then
    echo "" >> $tmp_file
    echo "# Start the proxy renewal server" >> $tmp_file
    echo "if [ ! -x $remote_BPRserver ]; then chmod u+x $remote_BPRserver; fi" >> $tmp_file
    echo "\`pwd\`/$remote_BPRserver \$job_pid $prnpoll $prnlifetime \${PBS_JOBID} &" >> $tmp_file
    echo "server_pid=\$!" >> $tmp_file
fi

echo "" >> $tmp_file
echo "# Wait for the user job to finish" >> $tmp_file
echo "wait \$job_pid" >> $tmp_file
echo "user_retcode=\$?" >> $tmp_file

if [ ! -z $proxyrenew ]
then
    echo "# Kill the watchdog when done" >> $tmp_file
    echo "sleep 1" >> $tmp_file
    echo "kill \$server_pid 2> /dev/null" >> $tmp_file
fi

if [ ! -z "$to_be_moved" ] ; then
    echo ""  >> $tmp_file
    echo "# Remove the staged files" >> $tmp_file
    echo "rm $to_be_moved" >> $tmp_file
fi

# We cannot remove the output files, as they have to be transferred back to the CE
# echo "cd .." >> $tmp_file
# echo "rm -rf \$HOME" >> $tmp_file

echo "" >> $tmp_file

echo "exit \$user_retcode" >> $tmp_file

# Exit if it was just a test
if [ "x$debug" == "xyes" ]
then
    exit 255
fi

# Let the wrap script be at least 1 second older than logfile
# for subsequent "find -newer" command to work
sleep 1


###############################################################
# Submit the script
###############################################################

if [ "x$workdir" != "x" ]; then
    cd $workdir
fi

if [ $? -ne 0 ]; then
    echo "Failed to CD to Initial Working Directory." >&2
    echo Error # for the sake of waiting fgets in blahpd
    rm -f $tmp_file
    exit 1
fi

datenow=`date +%Y%m%d`
jobID=`${pbs_binpath}/qsub $tmp_file 2> /dev/null` # actual submission
retcode=$?
if [ "$retcode" != "0" ] ; then
	rm -f $tmp_file
	exit 1
fi

if [ "x$pbs_nologaccess" != "xyes" -a "x$pbs_nochecksubmission" != "xyes" ]; then

# Don't trust qsub retcode, it could have crashed
# between submission and id output, and we would
# loose track of the job

# Search for the job in the logfile using job name

# Sleep for a while to allow job enter the queue
sleep 5


# find the correct logfile (it must have been modified
# *more* recently than the wrapper script)

logfile=""
jobID_log=""
log_check_retry_count=0
tfbasename="`basename ${tmp_file}`"
while [ "x$logfile" == "x" -a "x$jobID_log" == "x" ]; do

 cliretcode=0
 if [ "x$pbs_BLParser" == "xyes" ] ; then
     jobID_log=`echo BLAHJOB/$tmp_name| $BLClient -a $pbs_BLPserver -p $pbs_BLPport`
     cliretcode=$?
     if [ "x$jobID_log" != "x" ] ; then
        logfile=$datenow
     fi
 fi
 
 if [ "$cliretcode" == "1" -a "x$pbs_fallback" == "xno" ] ; then
  ${pbs_binpath}/qdel $jobID
  echo "Error: not able to talk with logparser on ${pbs_BLPserver}:${pbs_BLPport}" >&2
  echo Error # for the sake of waiting fgets in blahpd
  rm -f $tmp_file
  exit 1
 fi

 if [ "$cliretcode" == "1" -o "x$pbs_BLParser" != "xyes" ] ; then

     logfile=`find $logpath -type f -newer $tmp_file -exec grep -l "job name = $tmp_name" {} \;`
     if [ "x$logfile" != "x" ] ; then

       jobID_log=`grep "job name = $tmp_name" $logfile | awk -F";" '{ print $5 }'`
     fi
 fi

 if (( log_check_retry_count++ >= 12 )); then
     ${pbs_binpath}/qdel $jobID
     echo "Error: job not found in logs" >&2
     echo Error # for the sake of waiting fgets in blahpd
     rm -f $tmp_file
     exit 1
 fi

 let "bsleep = 2**log_check_retry_count"
 sleep $bsleep

done

if [ "$jobID_log" != "$jobID"  -a "x$jobID_log" != "x" ]; then
    echo "WARNING: JobID in log file is different from the one returned by qsub!" >&2
    echo "($jobID_log != $jobID)" >&2
    echo "I'll be using the one in the log ($jobID_log)..." >&2
    jobID=$jobID_log
fi

fi #end of if on $pbs_nologaccess

if [ "x$pbs_nologaccess" == "xyes" -o "x$pbs_nochecksubmission" == "xyes" ]; then
 logfile=$datenow
fi

# Compose the blahp jobID ("pbs/" + logfile + pbs jobid)
echo "BLAHP_JOBID_PREFIXpbs/`basename $logfile`/$jobID"

# Clean temporary files
cd $temp_dir
# DEBUG: cp $tmp_file /tmp
rm -f $tmp_file

# Create a softlink to proxy file for proxy renewal
if [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] ; then
    [ -d "$proxy_dir" ] || mkdir $proxy_dir
    ln -s $proxy_local_file $proxy_dir/$jobID.proxy
fi
                                                                                                                                                                                    
exit $retcode
