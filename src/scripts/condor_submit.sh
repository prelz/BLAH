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

usage_string="Usage: $0 -c <command> [-i <stdin>] [-o <stdout>] [-e <stderr>] [-x <x509userproxy>] [-v <environment>] [-s <yes | no>] [-- command_arguments]"


stgcmd="no"
stgproxy="yes"

proxyrenewald="${GLITE_LOCATION:-/opt/glite}/bin/BPRserver"

#default is to stage proxy renewal daemon 
proxyrenew=""

if [ ! -r "$proxyrenewald" ]
then
  unset proxyrenew
fi

proxy_dir=~/.blah_jobproxy_dir

workdir=$PWD

#default values for polling interval and min proxy lifetime
prnpoll=30
prnlifetime=0

BLClient="${GLITE_LOCATION:-/opt/glite}/bin/BLClient"

###############################################################
# Parse parameters
###############################################################
original_args=$@
while getopts "i:o:e:c:s:v:dw:q:n:rp:l:x:j:C:" arg 
do
    case "$arg" in
    i) stdin="$OPTARG" ;;
    o) stdout="$OPTARG" ;;
    e) stderr="$OPTARG" ;;
    v) envir="$OPTARG";;
    c) the_command="$OPTARG" ;;
    s) stgcmd="$OPTARG" ;;
    d) debug="yes" ;;
    w) workdir="$OPTARG";;
    q) queue="$OPTARG";;
    n) mpinodes="$OPTARG";;
    r) proxyrenew=yes"" ;;
    p) prnpoll="$OPTARG" ;;
    l) prnlifetime="$OPTARG" ;;
    x) proxy_string="$OPTARG" ;;
    j) creamjobid="$OPTARG" ;;
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
##############################################################
# Create wrapper script
###############################################################

# Get a suitable name for temp file
if [ "x$debug" != "xyes" ]
then
    if [ ! -z "$creamjobid"  ] ; then
                tmp_file=cream_${creamjobid}
        else
                tmp_file=`mktemp -q blahjob_XXXXXX`
        fi
        if [ $? -ne 0 ]; then
                echo Error
        exit 1
    fi
else
    # Just print to stderr if in debug
    tmp_file="/proc/$$/fd/2"
fi
tmp_file_2=${tmp_file}_2

# Create unique extension for filenames
# Not used yet!!!!  TBD
uni_uid=`id -u`
uni_pid=$$
uni_time=`date +%s`
uni_ext=$uni_uid.$uni_pid.$uni_time

# The sandbox for Condor script is the string which will go in the transfer_input_file field of condor submit file
# Fields of condor submit file tmp_file_2:

# a)executable: the executable is transferred by default, no need to put it in the sandbox.But
# the executable is not the command, is tmp_file!
blahpd_inputsandbox=$the_command

# b)BPRserver: BPRserver must be transferred and put into sandbox
# we must make a copy with custom-suffix that will be transferred after
if [ "x$proxyrenew" == "xyes" ] ; then
    if [ -r "$proxyrenewald" ] ; then
        remote_BPRserver=`basename $proxyrenewald`.$uni_ext
	cp -f $proxyrenewald $workdir/$remote_BPRserver
        blahpd_inputsandbox=$blahpd_inputsandbox,$workdir/${remote_BPRserver}
    else
        unset proxyrenew
    fi
fi
# c) user proxy: user proxy must be transferred and put into the sandbox:TBD
need_to_reset_proxy=no
proxy_remote_file=
if [ "x$stgproxy" == "xyes" ] ; then
    proxy_local_file=${workdir}"/"`basename "$proxy_string"`;
    [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] || proxy_local_file=$proxy_string
    [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] || proxy_local_file=/tmp/x509up_u`id -u`
    if [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] ; then
        proxy_remote_file=${tmp_file}.proxy
	if [ -z $blahpd_inputsandbox ] ; then
		blahpd_inputsandbox="${proxy_local_file}"
	else
        	blahpd_inputsandbox="${blahpd_inputsandbox},${proxy_local_file}"
        fi
	need_to_reset_proxy=yes
    fi
fi


# d)stdin: the stdin is transferred by default, no need to put it in the sandbox.Must be set
# in the tmp_file_2 condor submit file input entry. Doesn't need to have a unique name (??)
submit_stdin=$stdin

# e) stdout : must be set in the tmp_file_2 condor submit file output entry
# TBD if stdout ==stderr
if [ ! -z "$stdout" ] ; then
    if [ "${stdout:0:1}" != "/" ] ; then stdout=${workdir}/${stdout} ; fi
    #submit_stdout="$stdout.$uni_ext"
    submit_stdout=$stdout
fi
# f) stderr : must be set in the tmp_file_2 condor submit file error entry
if [ ! -z "$stderr" ] ; then
    if [ "${stderr:0:1}" != "/" ] ; then stderr=${workdir}/${stderr} ; fi
    #submit_stderr="$stderr.$uni_ext"
    submit_stderr=$stderr
fi

#  The tmp_file_2 is the condor submit file, the file actually submitted to Condor 
#  We use vanilla universe 

#  Remove any preexisting tmp_file_2
if [ -f $tmp_file_2 ] ; then
	rm -f $tmp_file_2
fi
echo "Executable = $tmp_file" >> $tmp_file_2
echo "Universe = vanilla"  >> $tmp_file_2
if [ ! -z $submit_stdin ] ; then
	echo "Input = $submit_stdin" >> $tmp_file_2
fi
if [ ! -z $submit_stdout ] ; then
	echo "Output = $submit_stdout" >> $tmp_file_2
	blahpd_outputsandbox=$submit_stdout
fi
if [ ! -z $submit_stderr ] ; then
	echo "error = $submit_stderr" >> $tmp_file_2
	blahpd_outputsandbox=$blahpd_outputsandbox,$submit_stderr
fi
if [ ! -z $blahpd_inputsandbox ] ; then
	echo "transfer_input_files = $blahpd_inputsandbox" >> $tmp_file_2
fi
if [ ! -z $blahpd_outputsandbox ] ; then
        echo "transfer_output_files = $blahpd_outputsandbox" >> $tmp_file_2
fi

echo "when_to_transfer_output = ON_EXIT" >> $tmp_file_2
echo "should_transfer_files = YES" >> $tmp_file_2
echo "queue 1 " >> $tmp_file_2

# The tmp_file is the wrapper around the executable, 
# used in the executable entry of tmp_file_2

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
end_of_preamble

#FORWARD REQS: NO NEED TO BE MODIFIED FOR CONDOR
#local batch system-specific file output must be added to the submit file
if [ ! -z $req_file ] ; then
    echo \#\!/bin/sh >> temp_req_script_$req_file
    cat $req_file >> temp_req_script_$req_file
    echo "source ${GLITE_LOCATION:-/opt/glite}/bin/pbs_local_submit_attributes.sh" >> temp_req_script_$req_file
    chmod +x temp_req_script_$req_file
    ./temp_req_script_$req_file  >> $tmp_file 2> /dev/null
    rm -f temp_req_script_$req_file
    rm -f $req_file
fi

#NO NEED TO BEMODIFIED FOR CONDOR (also if CONDOR supports the
# Environment attribute in condor submit file ...)
# Set the required environment variables (escape values with double quotes)
if [ "x$envir" != "x" ]  
then
    echo "" >> $tmp_file
    echo "# Setting the environment:" >> $tmp_file
    echo "`echo ';'$envir | sed -e 's/;[^=]*;/;/g' -e 's/;[^=]*$//g' | sed -e 's/;\([^=]*\)=\([^;]*\)/;export \1=\"\2\"/g' | awk 'BEGIN { RS = ";" } ; { print $0 }'`" >> $tmp_file
fi
#'#

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
    echo "# Prepare to clean up the watchdog when done" >> $tmp_file
    echo "sleep 1" >> $tmp_file
    echo "kill \$server_pid 2> /dev/null" >> $tmp_file
    echo "if [ -e \"$remote_BPRserver\" ]" >> $tmp_file
    echo "then" >> $tmp_file
    echo "    rm $remote_BPRserver" >> $tmp_file
    echo "fi" >> $tmp_file
fi

echo "# Clean up the proxy" >> $tmp_file
echo "if [ -e \"\$X509_USER_PROXY\" ]" >> $tmp_file
echo "then" >> $tmp_file
echo "    rm \$X509_USER_PROXY" >> $tmp_file
echo "fi" >> $tmp_file
echo "" >> $tmp_file

echo "exit \$user_retcode" >> $tmp_file

# Exit if it was just a test
if [ "x$debug" == "xyes" ]
then
    exit 255
fi

###############################################################
# Submit the script
###############################################################
curdir=`pwd`

if [ "x$workdir" != "x" ]; then
    cd $workdir
fi

if [ $? -ne 0 ]; then
    echo "Failed to CD to Initial Working Directory." >&2
    echo Error # for the sake of waiting fgets in blahpd
    rm -f $curdir/$tmp_file
    exit 1
fi

# Actual submission to condor to allow job enter the queue
chmod u+x $tmp_file;
if [ -z $queue ] ; then
	full_result=`condor_submit $tmp_file_2`
else
	full_result=`condor_submit $tmp_file_2 -r $queue`
fi 

if [ $? == "0" ] ; then
	jobID=`echo $full_result | egrep -o "[0-9]+{2,}"`		
fi
echo $jobID

# Compose the blahp jobID ("pbs/" + logfile + pbs jobid)
echo "BLAHP_JOBID_PREFIXcondor/`date +%Y%m%d`/$jobID"

# Clean temporary files
cd $curdir
rm -f $tmp_file
rm -f $tmp_file_2

# Create a softlink to proxy file for proxy renewal
if [ -r "$proxy_local_file" -a -f "$proxy_local_file" ] ; then
    [ -d "$proxy_dir" ] || mkdir $proxy_dir
    ln -s $proxy_local_file $proxy_dir/$jobID.proxy
fi                                                                                                                                                                                  
exit 0
