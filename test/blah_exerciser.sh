#!/bin/bash
#
# File:     blah_exerciser.sh
# Author:   Francesco Prelz (francesco.prelz@mi.infn.it)
#
# Revision history:
#     21-Dec-2007: Original release
#
# Description:
#     Script to exercise the BLAH daemon by submitting and monitoring jobs.
#
blex_desired_batchsystem="lsf"
blex_desired_queue="egee"
blex_blah_path="/opt/glite/bin/blahpd"
blex_iwd="/home/egeevouser/test_job"
blex_file_storage="/home/egeevouser/test_job_files"
blex_x509userproxy="$blex_iwd/test(){}|.proxy"
blex_checkpoint="$blex_iwd/blex.checkpoint"
blex_setup_glexec=0
blex_setup_sudo=0
blex_setup_sudo_user=
blex_proxy_in_submit_cmd=0
blex_usersubjectname=`openssl x509 -subject -noout -in $blex_x509userproxy`
#Remove leading 'subject= ', if any
blex_usersubjectname=${blex_usersubjectname#subject= }
blex_possible_job_types="DEFAULT"
blex_n_DEFAULT_jobs=1
blex_debug=1

. ./blah_exerciser_holdrelease_test.sh
. ./blah_exerciser_renewal_test.sh
. ./blah_exerciser_transfer_files_test.sh

blex_n_HOLDRELEASE_jobs=0
blex_n_RENEWAL_jobs=0

if [ -r ./blex_local_conf.sh ]
then
  . ./blex_local_conf.sh 
fi
# ----------8<-------END of configurable parameters--------8<----------8<

blex_submitted_jobs=""
blex_pending_submits=""
blex_pending_status=""
blex_pending_other=""
blex_n_success=0
blex_n_failure=0
blex_n_pending_commands=0
blex_loop_sleep_per_pending_command=2
blex_loop_sleep=0
blex_test_command=0
blex_start_submit=0
blex_end_submit=0
blex_end_all=0
blex_other_command_list=""

IFS=$' \t\n\r'

function blex_store_job_type ()
{
  local job_id
  local job_type
  local job_uniq
  local job_iduniq
  local listed_type
  local type_found

  job_id=${1:?"Missing Job ID argument to blex_store_job_type"}
  job_type=${2:?"Missing Job Type argument to blex_store_job_type"}
  job_uniq=${3:-""}

  type_found=0

  for listed_type in $blex_possible_job_types
  do
    if [ "$job_type" == "$listed_type" ]
    then
      type_found=1
      break
    fi
  done

  if [ $type_found -eq 0 ]
  then
    blex_possible_job_types="$blex_possible_job_types $job_type"
  fi

  job_iduniq="$job_id#$job_uniq"

  eval "blex_active_job_list_${job_type}=\"\${blex_active_job_list_${job_type}} $job_iduniq\"";
}

function blex_lookup_job_type ()
{
  local job_id
  local pop_id
  local found_iduniq
  local found_id
  local found_uniq
  local job_type
  local job_id_list
  local new_job_id_list

  job_id=${1:?"Missing Job ID argument to blex_lookup_job_type"}
  pop_id=${2:-0}

  blex_job_type=""

  for job_type in $blex_possible_job_types
  do
    eval "job_id_list=\"\${blex_active_job_list_${job_type}}\""
    new_job_id_list=""
    for found_iduniq in $job_id_list
    do
      found_id=${found_iduniq%\#*}
      found_uniq=${found_iduniq##*\#}
      if [ "$job_id" == "$found_id" ]
      then
        blex_job_type=$job_type
        blex_job_uniq=$found_uniq
        if [ $pop_id -eq 0 ]
        then
          break 2
        fi
      elif [ $pop_id -ne 0 ]
      then
        new_job_id_list="$new_job_id_list $found_iduniq"
      fi
    done
    if [ $pop_id -ne 0 ]
    then
      eval "blex_active_job_list_${job_type}=\"$new_job_id_list\""
    fi
  done
}

function blex_compose_submit_ad ()
{
  local lc_gridtype
  local lc_queue
  local lc_cmd
  local lc_args
  local lc_out
  local lc_err
  local lc_in
  local lc_env
  local lc_pwd

  lc_gridtype=${1:-"pbs"}
  lc_queue=${2:-"infinite"}
  lc_cmd=${3:-"/bin/sleep"}
  lc_args=${4:-"5"}
  lc_out=${5:-"/dev/null"}
  lc_err=${6:-"/dev/null"}
  lc_in=${7:-"/dev/null"}
  lc_env=${8:-""}
  lc_extra=${9:-""}

  blex_submit_ad="[ Cmd = \"$lc_cmd\"; Args = \"$lc_args\"; Out = \"$lc_out\";"
  blex_submit_ad="$blex_submit_ad Err = \"$lc_err\"; In = \"$lc_in\";"
  blex_submit_ad="$blex_submit_ad GridType = \"$lc_gridtype\"; Queue = \"$lc_queue\";"

  if [ "x$blex_x509userproxy" != "x" -a $blex_proxy_in_submit_cmd -gt 0 ]
  then
    blex_submit_ad="$blex_submit_ad x509userproxy = \"$blex_x509userproxy\";"
  fi
  if [ "x$blex_usersubjectname" != "x" -a $blex_proxy_in_submit_cmd -gt 0 ]
  then
    blex_submit_ad="$blex_submit_ad x509UserProxySubject = \"$blex_usersubjectname\";"
  fi
  if [ "x$blex_iwd" != "x" ]
  then
    blex_submit_ad="$blex_submit_ad Iwd = \"$blex_iwd\";"
  else
    lc_pwd=`pwd`;
    blex_submit_ad="$blex_submit_ad Iwd = \"$lc_pwd\";"
  fi

  if [ "x$lc_env" != "x" ]
  then
    blex_submit_ad="$blex_submit_ad Env = \"$lc_env\";"
  fi

  if [ "x$lc_extra" != "x" ]
  then
    blex_submit_ad="$blex_submit_ad $lc_extra"
  fi

  blex_submit_ad="$blex_submit_ad GridResource = \"blah\"; Stagecmd = TRUE; ]"
}

function blex_escape_spaces ()
{
  local string_to_escape
  string_to_escape="$*"

  blex_escaped_string=`echo $string_to_escape | sed -e 's/ /\\\\\\ /g'` 
}

function blex_compose_submit_command ()
{
  if [ "x$blex_sequence_no" == "x" ]
  then
    blex_sequence_no=1
  else
    let blex_sequence_no++
  fi
  blex_escape_spaces $blex_submit_ad
  blex_submit_command="BLAH_JOB_SUBMIT $blex_sequence_no $blex_escaped_string"
}

function blex_compose_status_command ()
{
  local lc_job
  lc_job=${1:?"Missing job argument in blex_compose_status_command"}
  if [ "x$blex_sequence_no" == "x" ]
  then
    blex_sequence_no=1
  else
    let blex_sequence_no++
  fi
  blex_status_command="BLAH_JOB_STATUS $blex_sequence_no $lc_job"
}

function blex_compose_other_command ()
{
  local lc_command
  lc_command=${1:?"Missing command argument in blex_compose_other_command"}
  if [ "x$blex_sequence_no" == "x" ]
  then
    blex_sequence_no=1
  else
    let blex_sequence_no++
  fi
  blex_other_command="$lc_command $blex_sequence_no"
}

function blex_enqueue_other_command ()
{
  local lc_command
  lc_command=${1:-"$blex_other_command"}

  if [ "x$lc_command" != "x" ]
  then
    blex_other_command_list="${blex_other_command_list}${blex_other_command}|"
  fi
}

function blex_submit_other_commands ()
{
  local lc_command
  local lc_sequence
  local lc_async_commands

  lc_async_commands=${1:-1}

  lc_command=${blex_other_command_list%%|*}

  while [ "x$lc_command" != "x" ]
  do
  
    if [ $lc_async_commands -gt 0 ]
    then
      lc_sequence=${lc_command#* }
      lc_sequence=${lc_sequence%% *}
    else
      lc_sequence="N/A"
    fi
 
    if [ $blex_debug -gt 0 ]
    then
      echo "DEBUG: submitting command: <$lc_command> (sequence: $lc_sequence)"
    fi

    echo $lc_command >&3
 
    read blex_other_result junk <&4
    if [ $blex_debug -gt 0 ]
    then
      echo "DEBUG: command returns : $blex_other_result $junk"
    fi
    if [ "$blex_other_result" == "S" ]
    then
      if [ $lc_async_commands -gt 0 ]
      then
        blex_pending_other="$blex_pending_other $lc_sequence"
      fi
    else
      echo "$0: ERROR sending command $lc_command. Status == $blex_other_result" 2>&1
    fi
    blex_other_command_list=${blex_other_command_list#*|}
    lc_command=""
    lc_command=${blex_other_command_list%%|*}
  done
  blex_other_command_list=""
}

function blex_wait_for_results ()
{
  local status
  local nres
  local junk
  local seqno
  local status
  local rest
  local new_pending_commands
  local n_pending_submits
  local new_job
  local job_id
  local blah_job_id
  local job_status
  local loop_done
  local submitted_job
  local new_submitted_jobs
  local interval
  local match_tail
  local i

  loop_done=0

  while (( loop_done == 0 ))
  do
    echo "RESULTS" >&3
# The 'junk' variable, along with CR in the IFS env variable, is used
# to discard the trailing CR.
    read status nres junk <&4
    if [ "$status" == "S" ]
    then
      for (( i=0 ; i<nres ; i++ ))
      do
        read seqno status rest<&4
#
# Measure time of completion if this is the currently active test command
#
  
        if [ $blex_test_command -eq $seqno ]
        then
          blex_test_command_end=$(awk "BEGIN { print systime() }")
          let interval=(blex_test_command_end-blex_test_command_start)
          let blex_loop_sleep_per_pending_command=interval/blex_n_pending_commands
          blex_test_command=0
          if [ $blex_debug -gt 0 ]
          then 
            echo "DEBUG: interval==$interval blex_n_pending_commands==$blex_n_pending_commands"
            echo "DEBUG: blex_loop_sleep_per_pending_command is now $blex_loop_sleep_per_pending_command"
          fi
        fi
#
# Check if this is a reply to a pending submit command.
        new_pending_commands=""
        n_pending_submits=0
        for pending_seqtyp in $blex_pending_submits
        do
          pending_seq=${pending_seqtyp%%|*}
          pending_typuniq=${pending_seqtyp#*|}
          pending_type=${pending_typuniq%%/*}
          pending_uniq=${pending_typuniq#*/}
          if [ $pending_seq -eq $seqno ]
          then
            if [ $blex_debug -gt 0 ]
            then
              echo "DEBUG: Submit command response: $seqno $status $rest"
            fi
            loop_done=1
            if [ $status -eq 0 ]
            then
              new_job=`echo $rest|sed -r -e 's/^.* ([^ \r\n]+)[ \r\n]*$/\1/'`
              blex_submitted_jobs="$blex_submitted_jobs $new_job"
              blex_store_job_type $new_job $pending_type $pending_uniq
            else
              echo "$0: ERROR: Job submit returns status $status: $rest" 2>&1
            fi
          else
            new_pending_commands="$new_pending_commands $pending_seqtyp"
            let n_pending_submits++
            loop_done=1
          fi
        done
        blex_pending_submits=$new_pending_commands
        if [ $n_pending_submits -eq 0 -a $blex_end_submit -eq 0 ]
        then
          blex_end_submit=$(awk "BEGIN { print systime() }")
        fi
#
# Check if this is a reply to any other (not submit or status) pending command.
        new_pending_commands=""
        for pending_seqcmd in $blex_pending_other
        do
          pending_seq=${pending_seqcmd%%|*}
          pending_cmd=${pending_seqcmd#*|}
          if [ $pending_seq -eq $seqno ]
          then
            loop_done=1
            if [ $status -eq 0 ]
            then
              if [ $blex_debug -gt 0 ]
              then
                echo "DEBUG: $pending_cmd returned success status $status: $rest"
              fi
            else
              echo "$0: ERROR: $pending_cmd returns status $status: $rest" 2>&1
            fi
          else
            new_pending_commands="$new_pending_commands $pending_seqcmd"
            loop_done=1
          fi
        done
        blex_pending_other=$new_pending_commands
#
# Check if this is a reply to a pending status command.
        new_pending_commands=""
        for pending_seq in $blex_pending_status
        do
          if [ $pending_seq -eq $seqno ]
          then
            loop_done=1
            if [ $status -eq 0 ]
            then
              job_id=${rest/*[Bb][Aa][Tt][Cc][Hh][Jj][Oo][Bb][Ii][Dd][[:space:]]=[[:space:]]\"/}
              job_id=${job_id/\"*/}
              blah_job_id=${rest/*[Bb][Ll][Aa][Hh][Jj][Oo][Bb][Ii][Dd][[:space:]]=[[:space:]]\"/}
              blah_job_id=${blah_job_id/\"*/}
              job_status=${rest/*[Jj][Oo][Bb][Ss][Tt][Aa][Tt][Uu][Ss][[:space:]]=[[:space:]]/}
              job_status=${job_status/[![:digit:]]*/}
              echo "JOB ID: $job_id JOB STATUS: $job_status"
              if [ $job_status -eq 4 -o $job_status -eq 3 -o $job_status -eq 6 ]
              then
#
# Remove job from active list
                new_submitted_jobs=""
                for submitted_job in $blex_submitted_jobs
                do
                  match_tail=$(( ${#submitted_job} - ${#job_id} ))
                  if [ ${submitted_job:match_tail:${#job_id}} != "$job_id" -a \
                       "$submitted_job" != "$blah_job_id" ]
                  then
                    new_submitted_jobs="$new_submitted_jobs $submitted_job"
                  else
#
# Add up statistics.
                    if [ $job_status -eq 4 ]
                    then
                      if [ $blex_debug -gt 0 ]
                      then
                        echo "DEBUG: status ad $rest"
                      fi
                      exit_code=${rest/*[Ee][Xx][Ii][Tt][Cc][Oo][Dd][Ee][[:space:]]=[[:space:]]/}
                      exit_code=${exit_code/[![:digit:]]*/}
                      blex_lookup_job_type $submitted_job 1 # Pop job ID
                      if [ "x$blex_job_type" != "x" ]
                      then
                        if [ $blex_debug -gt 0 ]
                        then
                          echo "DEBUG: Job $submitted_job of type $blex_job_type completed with status $exit_code"
                        fi
                      else
                        echo "WARNING: Job type of job $submitted_job could not be determined"
                      fi
                      blex_wrapup_result=1
                      if [ "x$blex_job_type" != "x" -a "$blex_job_type" != "DEFAULT" ]
                      then
                        if [ $blex_debug -gt 0 ]
                        then
                          echo "DEBUG: Wrapping job up with blex_wrap_job_up_$blex_job_type"
                        fi
                        eval "blex_wrap_job_up_$blex_job_type $submitted_job $blex_job_uniq $job_status"
                      fi
                      if [ $blex_wrapup_result -eq 0 ]
                      then
                        echo "$0: ERROR in validation script for $blex_job_type job" 2>&1
                        let blex_n_failure++
                      else
                        let blex_n_success++
                      fi
                    else
                      let blex_n_failure++
                    fi
                  fi
                done
                blex_submitted_jobs=$new_submitted_jobs
              else 
#
# Active job: do periodic maintenance, according to the job type
                for submitted_job in $blex_submitted_jobs
                do
                  match_tail=$(( ${#submitted_job} - ${#job_id} ))
                  if [ ${submitted_job:match_tail:${#job_id}} == "$job_id" -o \
                       "$submitted_job" == "$blah_job_id" ]
                  then
                    blex_lookup_job_type $submitted_job 
                    if [ "x$blex_job_type" != "x" -a "$blex_job_type" != "DEFAULT" ]
                    then
                      eval "blex_periodic_check_$blex_job_type $submitted_job $blex_job_uniq $job_status"
                    fi
                    break
                  fi
                done
              fi
            else
              echo "$0: ERROR: Job status returns status $status: $rest" 2>&1
            fi
          else
            new_pending_commands="$new_pending_commands $pending_seq"
            loop_done=1
          fi
        done
        blex_pending_status=$new_pending_commands
      done
    else
      loop_done=1
    fi
    if [ $loop_done -eq 0 ]
    then
      sleep 5
    fi
  done  
  if [ "x$blex_other_command_list" != "x" ]
  then
        blex_submit_other_commands
  fi
}

function blex_update_checkpoint
{
  local lc_checkpoint
  local lc_job_type
  local lc_job_list

  lc_checkpoint="$blex_checkpoint.new"

  echo "blex_pipin='$blex_pipin'" > $lc_checkpoint
  echo "blex_pipout='$blex_pipout'" >> $lc_checkpoint
  echo "blex_start_submit='$blex_start_submit'" >> $lc_checkpoint
  echo "blex_end_submit='$blex_end_submit'" >> $lc_checkpoint
  echo "blex_n_success='$blex_n_success'" >> $lc_checkpoint
  echo "blex_n_failure='$blex_n_failure'" >> $lc_checkpoint
  echo "blex_initial_submitted_jobs='$blex_initial_submitted_jobs'" >> $lc_checkpoint
  echo "blex_submitted_jobs='$blex_submitted_jobs'" >> $lc_checkpoint
  echo "blex_pending_submits='$blex_pending_submits'" >> $lc_checkpoint
  echo "blex_pending_status='$blex_pending_status'" >> $lc_checkpoint
  echo "blex_pending_other='$blex_pending_other'" >> $lc_checkpoint
  echo "blex_loop_sleep_per_pending_command='$blex_loop_sleep_per_pending_command'" >> $lc_checkpoint
  echo "blex_possible_job_types='$blex_possible_job_types'" >> $lc_checkpoint
  for lc_job_type in $blex_possible_job_types
  do
    eval "lc_job_list=\"\${blex_active_job_list_${lc_job_type}}\""
    echo "blex_active_job_list_${lc_job_type}='$lc_job_list'" >> $lc_checkpoint
  done

  /bin/mv $lc_checkpoint $blex_checkpoint
}


blex_initial_submitted_jobs=0

if [ -r $blex_checkpoint ]
then
  . $blex_checkpoint
  if [ -r $blex_pipout -a -w $blex_pipin ]
  then
#   Try attaching to the running blahpd.
    exec 3<>$blex_pipin
    exec 4<>$blex_pipout
#   Is it responding OK ? Try twice to clear
#   communications.
    echo "VERSION" >&3
    read <&4
    echo "VERSION" >&3
    read <&4
    if [ "${REPLY:0:1}" != "S" -o "${1:0:5}" == "fresh" ]
    then
#     Wrong reply or fresh start requested. Try terminating the blahpd
#     and starting a new one.
      echo "QUIT" >&3
      /bin/rm $blex_checkpoint
      blex_initial_submitted_jobs=0
    else
      echo "REPLY == $REPLY"
      blex_n_submitted_jobs=0
      for submitted_job in $blex_submitted_jobs
      do
        let blex_n_submitted_jobs++
      done
      if [ $blex_debug -gt 0 ]
      then
        echo "DEBUG: found blex_n_submitted_jobs == $blex_n_submitted_jobs"
      fi
    fi
  else
#   Start afresh.
    blex_initial_submitted_jobs=0
  fi
fi

if [ $blex_initial_submitted_jobs -le 0 ]
then
#
# Create a pair of named pipes for communication with BLAH
  blex_pipin=`mktemp -t blexpipe.XXXXXXXXXX`
  if [ $? -ne 0 ]
  then
    echo "$0: ERROR creating temp file." 2>&1
    exit 1
  fi
  blex_pipout=`mktemp -t blexpipe.XXXXXXXXXX`
  if [ $? -ne 0 ]
  then
    /bin/rm $blex_pipin
    echo "$0: ERROR creating temp file." 2>&1
    exit 1
  fi
    
  mknod $blex_pipin.tmp p
  if [ $? -ne 0 ]
  then
    echo "$0: ERROR creating named pipe $blex_pipin" 2>&1
    /bin/rm $blex_pipin
    /bin/rm $blex_pipout
    exit 1
  else
    /bin/mv $blex_pipin.tmp $blex_pipin
  fi
  
  mknod $blex_pipout.tmp p
  if [ $? -ne 0 ]
  then
    echo "$0: ERROR creating named pipe $blex_pipout" 2>&1
    /bin/rm $blex_pipin
    /bin/rm $blex_pipout
    exit 1
  else
    /bin/mv $blex_pipout.tmp $blex_pipout
  fi
  
  exec 3<>$blex_pipin
  exec 4<>$blex_pipout
  
  $blex_blah_path > $blex_pipout < $blex_pipin &
  if [ $? -ne 0 ]
  then
    echo "$0: ERROR executing $blex_blah_path" 2>&1
    /bin/rm $blex_pipin
    /bin/rm $blex_pipout
    exit 1
  fi
  
  read <&4
  echo "REPLY == $REPLY"

  blex_n_submitted_jobs=0  

  if [ $blex_setup_glexec -gt 0 ]
  then
    blex_other_command="CACHE_PROXY_FROM_FILE 1 $blex_x509userproxy"
    blex_enqueue_other_command
    blex_other_command="USE_CACHED_PROXY 1"
    blex_enqueue_other_command
    blex_submit_other_commands 0 # Synchronous commands
  elif [ $blex_setup_sudo -gt 0 -a "x$blex_setup_sudo_user" != "x" ]
  then
    blex_other_command="BLAH_SET_SUDO_ID $blex_setup_sudo_user"
    blex_enqueue_other_command
    blex_submit_other_commands 0 # Synchronous commands
  fi
  
  blex_start_submit=$(awk "BEGIN { print systime() }")
  for blex_job_type in $blex_possible_job_types
  do
    eval "blex_n_jobs=\$blex_n_${blex_job_type}_jobs"
    for (( i=0 ; i<blex_n_jobs ; i++ ))
    do
      if [ "$blex_job_type" == "DEFAULT" ]
      then
        blex_compose_submit_ad $blex_desired_batchsystem $blex_desired_queue
        blex_file_unique=""
      else
        eval "blex_compose_submit_ad_${blex_job_type} $blex_desired_batchsystem $blex_desired_queue"
      fi
  
      if [ "x$blex_submit_ad" != "x" ]
      then
        blex_compose_submit_command
        
        if [ $blex_debug -gt 0 ]
        then 
          echo "DEBUG: submit command: <$blex_submit_command>"
        fi
        
        echo $blex_submit_command >&3
        
        read blex_submit_result junk <&4
        if [ "$blex_submit_result" == "S" ]
        then
          blex_pending_submits="$blex_pending_submits $blex_sequence_no|$blex_job_type/$blex_file_unique"
          let blex_n_submitted_jobs++
        else
          echo "$0: ERROR submitting job $blex_submit_ad" 2>&1
        fi
      else
        echo "$0: ERROR creating submit ad for $blex_job_type job" 2>&1
      fi
      # Give time for submit not to clog the local system
      sleep 1
    done
  done
  
  blex_initial_submitted_jobs=$blex_n_submitted_jobs
  blex_update_checkpoint
fi

while (( blex_n_submitted_jobs > 0 ))
do
  blex_wait_for_results
  blex_n_submitted_jobs=0
  for submitted_job in $blex_submitted_jobs
  do
    let blex_n_submitted_jobs++

    blex_compose_status_command $submitted_job

    if [ $blex_debug -gt 0 ]
    then 
      echo "DEBUG: status command: <$blex_status_command>"
    fi

    echo $blex_status_command >&3
  
    read blex_status_result junk <&4
    if [ "$blex_status_result" == "S" ]
    then
      blex_pending_status="$blex_pending_status $blex_sequence_no"
      if [ $blex_test_command -eq 0 ]
      then
        blex_test_command=$blex_sequence_no
        blex_test_command_start=$(awk "BEGIN { print systime() }")
      fi
    else
      echo "$0: ERROR requesting status of job $submitted_job" 2>&1
    fi
    blex_update_checkpoint

  done

  if [ $blex_debug -gt 0 ]
  then 
    echo "DEBUG: $blex_n_submitted_jobs job(s) left."
  fi
#
# Wait for a time proportional to the number of *pending* commands
#
  blex_loop_sleep=0
  blex_n_pending_commands=0
  for junk in $blex_pending_submits
  do
    let blex_loop_sleep+=blex_loop_sleep_per_pending_command
    let blex_n_pending_commands++
  done
  for junk in $blex_pending_status
  do
    let blex_loop_sleep+=blex_loop_sleep_per_pending_command
    let blex_n_pending_commands++
  done
  for junk in $blex_pending_other
  do
    let blex_loop_sleep+=blex_loop_sleep_per_pending_command
    let blex_n_pending_commands++
  done
  if [ $blex_n_submitted_jobs -gt 0 ]
  then
    if [ $blex_debug -gt 0 ]
    then
      echo "DEBUG: sleeping $blex_loop_sleep seconds and continuing"
    fi
    sleep $blex_loop_sleep
  fi
done

blex_end_all=$(awk "BEGIN { print systime() }")

echo "QUIT" >&3

/bin/rm $blex_checkpoint

exec 3>&-
exec 4<&-

/bin/rm -f $blex_pipin $blex_pipout

echo "$0 exiting: Submitted $blex_initial_submitted_jobs jobs. $blex_n_success succeeded. $blex_n_failure failed."
echo "Took $(( blex_end_submit - blex_start_submit )) seconds to submit."
echo "Took $(( blex_end_all - blex_start_submit )) seconds to complete."

exit 0
