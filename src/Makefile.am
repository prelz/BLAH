# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.1 2004/09/14 15:20:54 pandreet Exp $ 
#
# File:           Makefile.am
#
# Author(s):      Francesco Prelz ($Author: pandreet $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 16-Apr-2004  Created 
#

INCLUDES = -I. 

bin_PROGRAMS = blahpd blahpd_daemon

common_sources = console.c job_status.c resbuffer.c server.c commands.c classad_c_helper.C

blahpd_SOURCES = main.c $(common_sources)

blahpd_daemon_SOURCES = main_daemon.c $(common_sources)

blahpd_LDADD = -lclassad

blahpd_daemon_LDADD = $(blahpd_LDADD)

noinst_HEADERS = blahpd.h classad_c_helper.h commands.h job_status.h resbuffer.h server.h console.h

bin_SCRIPTS = pbs_cancel.sh pbs_status.sh pbs_submit.sh lsf_cancel.sh lsf_status.sh lsf_submit.sh

EXTRA_DIST = $(bin_SCRIPTS)
