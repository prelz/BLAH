#!/bin/bash
#
# File:     blah_exerciser_holdrelease_test.sh
# Author:   Francesco Prelz (francesco.prelz@mi.infn.it)
#
# Revision history:
#     10-Apr-2007: Original release
#
# Description:
#     Functions to set up, periodically maintain and reap
#     jobs for hold and release tests.
#

blex_possible_job_types="$blex_possible_job_types HOLDRELEASE"
blex_n_HOLDRELEASE_jobs=3
blex_holdrelease_test_n_sleep=120
blex_holdrelease_test_last_release=0

function blex_compose_submit_ad_HOLDRELEASE ()
{
  local lc_gridtype
  local lc_queue
  local lc_args
  local lc_out
  local lc_err
  local lc_in
  local lc_env
  local lc_pwd
  local lc_holdrelease_check_script
  
  lc_gridtype=${1:-"pbs"}
  lc_queue=${2:-"infinite"}

  blex_submit_ad=""

  lc_holdrelease_check_script="blah_exerciser_holdrelease_test_check_script.sh"

  if [ ! -e "$blex_iwd/$lc_holdrelease_check_script" ]
  then
    cat > $blex_iwd/$lc_holdrelease_check_script << EOTS
#!/bin/sh
#
# Dummy script. Prints the system time, sleeps for a given number
# of seconds, then prints the system time again.
# This can be used to verify that the job is resumed correctly.
#
echo \`awk "BEGIN { print systime() }"\` 
sleep $blex_holdrelease_test_n_sleep
echo \`awk "BEGIN { print systime() }"\` 
EOTS

    if [ $? -ne 0 ]
    then
      echo "$0: ERROR creating hold/release check script at $blex_iwd/$lc_holdrelease_check_script" 2>&1
      return
    fi
fi
  
# Make script executable in case it is in a shared dir
chmod +x $blex_iwd/$lc_holdrelease_check_script

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

  if [ $blex_setup_glexec -gt 0 -o $blex_setup_sudo -gt 0 ]
  then
    chmod g+rw $blex_file_storage/stdout_$blex_file_unique
    chmod g+rw $blex_file_storage/stderr_$blex_file_unique
  fi

  blex_compose_submit_ad $lc_gridtype $lc_queue \
                         $blex_iwd/$lc_holdrelease_check_script "-" \
                         $blex_file_storage/stdout_$blex_file_unique \
                         $blex_file_storage/stderr_$blex_file_unique 
}

function blex_periodic_check_HOLDRELEASE ()
{
  local lc_job_id
  local lc_job_uniq
  local lc_job_status
  local lc_random
  local lc_span

  blex_other_command=""

  lc_job_id=${1:?"Missing Job ID argument to blex_periodic_check_HOLDRELEASE"}
  lc_job_uniq=${2:?"Missing Job Unique String argument to blex_periodic_check_HOLDRELEASE"}
  lc_job_status=${3:-1} # Default to IDLE.

  if [ $lc_job_status -ne 1 -a $lc_job_status -ne 2 -a $lc_job_status -ne 5 ]
  then
#   Job status is neither IDLE (1) nor RUNNING (2) nor HELD (5)
#   Nothing to do.
    return
  fi

  if [ $lc_job_status -eq 5 ]
  then
#   Job is HELD. Release it:
    blex_compose_other_command BLAH_JOB_RESUME
    blex_other_command="$blex_other_command $lc_job_id" 
    blex_enqueue_other_command
    blex_holdrelease_test_last_release=`awk "BEGIN { print systime() }"` 
  else 
    lc_span=`awk "BEGIN { print systime() }"`
    let lc_span-=blex_holdrelease_test_last_release
    if [ $RANDOM -gt 20000 -a $lc_span -gt $blex_holdrelease_test_n_sleep ]
    then
      blex_compose_other_command BLAH_JOB_HOLD
      blex_other_command="$blex_other_command $lc_job_id" 
      blex_enqueue_other_command
    fi
  fi
}

function blex_wrap_job_up_HOLDRELEASE ()
{
  local lc_job_id
  local lc_job_uniq
  local lc_job_status
  local lc_start
  local lc_end
  local lc_allowed_diff
  local lc_delta
  local lc_diff
  local lc_fcheck

  blex_wrapup_result=1

  lc_job_id=${1:?"Missing Job ID argument to blex_periodic_check_HOLDRELEASE"}
  lc_job_uniq=${2:?"Missing Job Unique String argument to blex_periodic_check_HOLDRELEASE"}
  lc_job_status=${3:-4} # Default to COMPLETED.

  lc_start=""
  lc_end=""
  exec 5<>"$blex_file_storage/stdout_${lc_job_uniq}"
  read lc_start <&5
  read lc_end   <&5
  exec 5<&-

  lc_fcheck=""

  if [ "x$lc_start" == "x" ]
  then
    lc_fcheck="${lc_fcheck}ERROR: Hold test job start time not found. "    
  fi
  if [ "x$lc_end" == "x" ]
  then
    lc_fcheck="${lc_fcheck}ERROR: Hold test job end time not found. "    
  fi

  if [ "x$lc_fcheck" == "x" ]
  then
#   One percent tolerance
    lc_allowed_delta=$(( blex_holdrelease_test_n_sleep - blex_holdrelease_test_n_sleep/100 - 1 ))
    lc_delta=$(( lc_end - lc_start ))
    if [ $lc_delta -lt $lc_allowed_delta ]
    then
      lc_fcheck="${lc_fcheck}ERROR: Time difference ($lc_delta s) is less than $lc_allowed_delta s."
    fi
  fi

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
