#!/bin/bash
#  File:     blah_load_config.sh 
#
#  Author:   Francesco Prelz 
#  e-mail:   Francesco.Prelz@mi.infn.it
#
#
# Copyright (c) Members of the EGEE Collaboration. 2004. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
# 
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
# 
#     http://www.apache.org/licenses/LICENSE-2.0 
# 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.
#
# Load the BLAH configuration. The search procedure is the same as in
# src/config.c

if [ "x$BLAHPD_LOCATION" != "x" -a -d "${BLAHPD_LOCATION}/bin" ]; then
  blah_bin_directory="${BLAHPD_LOCATION}/bin"
elif [ -d "${GLITE_LOCATION:-/opt/glite}/bin" ]; then
  blah_bin_directory="${GLITE_LOCATION:-/opt/glite}/bin"
else
  blah_bin_directory="/usr/bin"
fi
if [ "x$BLAHPD_LOCATION" != "x" -a -d "${BLAHPD_LOCATION}/sbin" ]; then
  blah_sbin_directory="${BLAHPD_LOCATION}/sbin"
elif [ -d "${GLITE_LOCATION:-/opt/glite}/sbin" ]; then
  blah_sbin_directory="${GLITE_LOCATION:-/opt/glite}/sbin"
else
  blah_sbin_directory="/usr/sbin"
fi
if [ "x$BLAHPD_LOCATION" != "x" -a -d "${BLAHPD_LOCATION}/libexec" ]; then
  blah_libexec_directory="${BLAHPD_LOCATION}/libexec"
elif [ -d "${GLITE_LOCATION:-/opt/glite}/libexec" ]; then
  blah_libexec_directory="${GLITE_LOCATION:-/opt/glite}/libexec"
else
  blah_libexec_directory="/usr/libexec"
fi

# Let blah_bin_directory be overridden in the config file.

if [ -r "$BLAHPD_CONFIG_LOCATION" ]; then
  . $BLAHPD_CONFIG_LOCATION
elif [ -r "${GLITE_LOCATION:-/opt/glite}/etc/batch_gahp.config" ]; then
  . ${GLITE_LOCATION:-/opt/glite}/etc/batch_gahp.config
elif [ -r "${BLAHPD_LOCATION}/etc/batch_gahp.config" ]; then
#   This will default to /etc/batch_gahp.config if BLAHPD_LOCATION is unset.
  . ${BLAHPD_LOCATION}/etc/batch_gahp.config
elif [ -r "${GLITE_LOCATION:-/opt/glite}/etc/blah.config" ]; then
  . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config
elif [ -r "${BLAHPD_LOCATION}/etc/blah.config" ]; then
#   This will default to /etc/blah.config if BLAHPD_LOCATION is unset.
  . ${BLAHPD_LOCATION}/etc/blah.config
fi

