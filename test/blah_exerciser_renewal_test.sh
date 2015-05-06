#!/bin/bash
#
# File:     blah_exerciser_renewal_test.sh
# Author:   Francesco Prelz (francesco.prelz@mi.infn.it)
#
# Revision history:
#     7-Apr-2007: Original release
#
# Description:
#     Functions to set up, periodically maintain and reap
#     jobs for proxy renewal tests.
#

blex_possible_job_types="$blex_possible_job_types RENEWAL"
blex_n_RENEWAL_jobs=3

function blex_compose_submit_ad_RENEWAL ()
{
  local lc_gridtype
  local lc_queue
  local lc_args
  local lc_out
  local lc_err
  local lc_in
  local lc_env
  local lc_pwd
  local lc_proxy_check_script
  local lc_max_time
  local lc_n_iter
  local lc_n_sleep
  
  lc_gridtype=${1:-"pbs"}
  lc_queue=${2:-"infinite"}

  blex_submit_ad=""

  lc_max_time=300
  lc_n_iter=20
  lc_n_sleep=$(( lc_max_time/lc_n_iter + 1 ))
  lc_proxy_check_script="blah_exerciser_renewal_test_check_script.sh"

  if [ ! -e "$blex_iwd/$lc_proxy_check_script" ]
  then
    cat > $blex_iwd/$lc_proxy_check_script << EOTS
#!/bin/sh
#
# Script to check and print the mtime and validity of the current
# user proxy. The stdout format is:
# system unix time - X509_USER_PROXY mtime - output of grid-proxy-info -timeleft
#
globus_stuff=\${GLOBUS_LOCATION:-/opt/globus}
#
for (( i=0 ; i<$lc_n_iter ; i++ ))
do
  echo -n \`awk "BEGIN { print systime() }"\` 
  echo -n " - " 
  echo -n \`stat -c %Y \$X509_USER_PROXY\` 
  echo -n " - " 
  echo \`\$globus_stuff/bin/grid-proxy-info -timeleft 2>/dev/null\`
  sleep $lc_n_sleep
done
EOTS

    if [ $? -ne 0 ]
    then
      echo "$0: ERROR creating proxy check script at $blex_iwd/$lc_proxy_check_script" 2>&1
      return
    fi
fi

# Make script executable in case it is in a shared dir
chmod +x $blex_iwd/$lc_proxy_check_script
  
#
# Create unique files for stdout, stderr storage
  
  lc_stdout_file=`mktemp -q $blex_file_storage/stdout_XXXXXX`
  if [ $? -ne 0 ]
  then
    echo "$0: ERROR creating unique file for stdout" 2>&1
    return
  fi

  blex_file_unique=${lc_stdout_file##*/stdout_};

  echo -n "" > $blex_file_storage/stderr_$blex_file_unique
  if [ $? -ne 0 ]
  then
    unlink $lc_stdout_file
    echo "$0: ERROR creating unique file for stderr ($blex_file_storage/stderr_$blex_file_unique)" 2>&1
    return
  fi

  cp $blex_x509userproxy $blex_file_storage/proxy_$blex_file_unique
  chmod 600 $blex_file_storage/proxy_$blex_file_unique

  if [ $? -ne 0 ]
  then
    unlink $lc_stdout_file
    unlink $blex_file_storage/stderr_$blex_file_unique
    echo "$0: ERROR copying proxy for job" 2>&1
    return
  fi

  if [ $blex_setup_glexec -gt 0 -o $blex_setup_sudo -gt 0 ]
  then
#    chmod g+r $blex_file_storage/proxy_$blex_file_unique
    chmod g+rw $blex_file_storage/stdout_$blex_file_unique
    chmod g+rw $blex_file_storage/stderr_$blex_file_unique
  fi

  blex_compose_submit_ad $lc_gridtype $lc_queue \
                         $blex_iwd/$lc_proxy_check_script "-" \
                         $blex_file_storage/stdout_$blex_file_unique \
                         $blex_file_storage/stderr_$blex_file_unique 
}

function blex_periodic_check_RENEWAL ()
{
  local lc_job_id
  local lc_job_uniq
  local lc_job_status
  local lc_random

  blex_other_command=""

  lc_job_id=${1:?"Missing Job ID argument to blex_periodic_check_RENEWAL"}
  lc_job_uniq=${2:?"Missing Job Unique String argument to blex_periodic_check_RENEWAL"}
  lc_job_status=${3:-1} # Default to IDLE.

  if [ $lc_job_status -eq 2 -o $lc_job_status -eq 1 -a $RANDOM -gt 20000 ]
  then
    if [ -r $blex_file_storage/proxy_$lc_job_uniq ]
    then
      blex_compose_other_command BLAH_JOB_REFRESH_PROXY
      blex_other_command="$blex_other_command $lc_job_id $blex_file_storage/proxy_$lc_job_uniq" 
      blex_enqueue_other_command
    fi
  fi
}

function blex_wrap_job_up_RENEWAL ()
{
  local lc_job_id
  local lc_job_uniq
  local lc_job_status
  local lc_fcheck

  blex_wrapup_result=1

  lc_job_id=${1:?"Missing Job ID argument to blex_periodic_check_RENEWAL"}
  lc_job_uniq=${2:?"Missing Job Unique String argument to blex_periodic_check_RENEWAL"}
  lc_job_status=${3:-4} # Default to COMPLETED.

  /bin/rm -f $blex_file_storage/proxy_${lc_job_uniq}* 

  if [ $? -ne 0 ]
  then
    echo "$0: ERROR deleting proxies $blex_file_storage/proxy_${lc_job_uniq}*" 2>&1
    return
  fi
  
  lc_fcheck=`awk -F ' ' -v pfile="$blex_file_storage/stdout_${lc_job_uniq}" ' 
    BEGIN {
     last_mtime = 0
     mtime_increases = 0
     mtime_monotonic = 1
    }
    /[0-9]+ *- *[0-9]+ *- *[0-9]+/ {
     if (last_mtime == 0) { last_mtime = $3 }
     else if ( $3 > last_mtime ) { last_mtime = $3; mtime_increases = 1 }
     else if ( $3 < last_mtime ) { mtime_monotonic = 0 } 
    }
    END {
     if (mtime_increases == 0) { print "ERROR with " pfile " proxy: mtime does not increase\n" }
     if (mtime_monotonic == 0) { print "ERROR with " pfile " proxy: mtime decreases \n" }
    }
  ' $blex_file_storage/stdout_${lc_job_uniq}`

  if [ "x$lc_fcheck" != "x" ]
  then
    echo "$0: $lc_fcheck" 2>&1
    if [ $blex_debug -gt 0 ]
    then
      echo "Job STDOUT: ---------------------------------------------"
      cat $blex_file_storage/stdout_${lc_job_uniq}
      echo "Job STDERR: ---------------------------------------------"
      cat $blex_file_storage/stderr_${lc_job_uniq}
      echo "---------------------------------------------------------"
    fi
    blex_wrapup_result=0
  fi

  unlink $blex_file_storage/stdout_${lc_job_uniq}
  unlink $blex_file_storage/stderr_${lc_job_uniq}
}
