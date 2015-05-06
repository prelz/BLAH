#!/bin/bash
#
# File:     blah_exerciser_transfer_files_test.sh
# Author:   Francesco Prelz (francesco.prelz@mi.infn.it)
#
# Revision history:
#    27-Apr-2010: Original release
#
# Description:
#     Functions to set up, periodically maintain and reap
#     jobs for file transfer tests.
#

blex_possible_job_types="$blex_possible_job_types TRANSFERFILES"
blex_n_TRANSFERFILES_jobs=3

function blex_compose_submit_ad_TRANSFERFILES ()
{
  local lc_gridtype
  local lc_queue
  local lc_args
  local lc_stdout_file
  local lc_transferfiles_check_script
  local lc_transfer_directives
  
  lc_gridtype=${1:-"pbs"}
  lc_queue=${2:-"infinite"}

  blex_submit_ad=""

  lc_transferfiles_check_script="blah_exerciser_transfer_files_test_check_script.sh"

  if [ ! -e "$blex_iwd/$lc_transferfiles_check_script" ]
  then
    cat > $blex_iwd/$lc_transferfiles_check_script << EOTS
#!/bin/sh
#
# Script to copy an input file to an output file to test BLAH file
# trasfer features.
#
myself=`uname -n`;
/bin/echo "Added by \$myself on:" > output_file
date >> output_file
cat input_file1 >> output_file
cat input_file2 >> output_file
EOTS

    if [ $? -ne 0 ]
    then
      echo "$0: ERROR creating file transfer check script at $blex_iwd/$lc_transferfiles_check_script" 2>&1
      return
    fi
fi

# Make script executable in case it is in a shared dir
chmod +x $blex_iwd/$lc_transferfiles_check_script
  
#
# Create unique files for output, stdout, stderr storage
  
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

  echo -n "" > $blex_file_storage/output_$blex_file_unique
  if [ $? -ne 0 ]
  then
    unlink $lc_stdout_file
    unlink $blex_file_storage/stderr_$blex_file_unique
    echo "$0: ERROR creating unique file for output ($blex_file_storage/output_$blex_file_unique)" 2>&1
    return
  fi

  if [ $blex_setup_glexec -gt 0 -o $blex_setup_sudo -gt 0 ]
  then
    chmod g+rw $blex_file_storage/stdout_$blex_file_unique
    chmod g+rw $blex_file_storage/stderr_$blex_file_unique
    chmod g+rw $blex_file_storage/output_$blex_file_unique
  fi

  if [ ! -r $blex_file_storage/input_file1 ]
  then
    cat > $blex_file_storage/input_file1 << EOTI
La Vispa Teresa
avea fra l'erbetta
a volo sorpresa
gentil farfalletta.
EOTI
  fi

  if [ ! -r $blex_file_storage/input_file2 ]
  then
    cat > $blex_file_storage/input_file2 << EOTF

E tutta giuliva
stringendola viva
gridava distesa
"l'ho presa, l'ho presa".
EOTF
  fi

  lc_transfer_directives="TransferInput = \"$blex_file_storage/input_file1,$blex_file_storage/input_file2\"; TransferOutput = \"output_file\"; TransferOutputRemaps = \"output_file=$blex_file_storage/output_$blex_file_unique\";"

  blex_compose_submit_ad $lc_gridtype $lc_queue \
                         $blex_iwd/$lc_transferfiles_check_script "-" \
                         $blex_file_storage/stdout_$blex_file_unique \
                         $blex_file_storage/stderr_$blex_file_unique \
                         "" "" "$lc_transfer_directives"
}

function blex_periodic_check_TRANSFERFILES ()
{
  local lc_job_id
  local lc_job_uniq
  local lc_job_status
  local lc_random

  blex_other_command=""

  lc_job_id=${1:?"Missing Job ID argument to blex_periodic_check_TRANSFERFILES"}
  lc_job_uniq=${2:?"Missing Job Unique String argument to blex_periodic_check_TRANSFERFILES"}
  lc_job_status=${3:-1} # Default to IDLE.

# Nothing to check.

}

function blex_wrap_job_up_TRANSFERFILES ()
{
  local lc_job_id
  local lc_job_uniq
  local lc_job_status
  local lc_fcheck
  local lc_check_tutta
  local lc_check_vispa
  local lc_check_added

  blex_wrapup_result=1

  lc_job_id=${1:?"Missing Job ID argument to blex_periodic_check_TRANSFERFILES"}
  lc_job_uniq=${2:?"Missing Job Unique String argument to blex_periodic_check_TRANSFERFILES"}
  lc_job_status=${3:-4} # Default to COMPLETED.

  grep "^Added by" $blex_file_storage/output_${lc_job_uniq} > /dev/null 2>&1
  lc_check_added=$?
  grep "^La Vispa" $blex_file_storage/output_${lc_job_uniq} > /dev/null 2>&1
  lc_check_vispa=$?
  grep "^E tutta" $blex_file_storage/output_${lc_job_uniq} > /dev/null 2>&1
  lc_check_tutta=$?

  if [ $lc_check_added -ne 0 -o $lc_check_vispa -ne 0 -o $lc_check_tutta -ne 0 ]
  then
    echo "$0: Cannot find text in output file." 2>&1
    if [ $blex_debug -gt 0 ]
    then
      echo "Job STDOUT: ---------------------------------------------"
      cat $blex_file_storage/stdout_${lc_job_uniq}
      echo "Job STDERR: ---------------------------------------------"
      cat $blex_file_storage/stderr_${lc_job_uniq}
      echo "Job OUTPUT FILE: ---------------------------------------------"
      cat $blex_file_storage/output_${lc_job_uniq}
      echo "---------------------------------------------------------"
    fi
    blex_wrapup_result=0
  fi

  unlink $blex_file_storage/stdout_${lc_job_uniq}
  unlink $blex_file_storage/stderr_${lc_job_uniq}
  unlink $blex_file_storage/output_${lc_job_uniq}
}
