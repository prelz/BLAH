# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.5 2005/03/23 14:29:49 drebatto Exp $ 
#
# File:           Makefile.am
#
# Author(s):      Francesco Prelz ($Author: drebatto $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 16-Apr-2004  Created 
#

INCLUDES = -I. $(CLASSAD_CFLAGS) $(GLOBUS_NOTHR_CFLAGS)

SUBDIRS = scripts

bin_PROGRAMS = blahpd blahpd_daemon BPRclient BPRserver

common_sources = console.c job_status.c resbuffer.c server.c commands.c classad_c_helper.C

blahpd_SOURCES = main.c $(common_sources)

blahpd_daemon_SOURCES = main_daemon.c $(common_sources)

blahpd_LDADD = $(CLASSAD_LIBS)

blahpd_daemon_LDADD = $(blahpd_LDADD)

BPRclient_SOURCES = BPRclient.c BPRcomm.c tokens.c

BPRclient_LDADD = \
$(GLOBUS_GSS_NOTHR_LIBS) $(GLOBUS_SSL_NOTHR_LIBS)

BPRserver_SOURCES = BPRserver.c BPRcomm.c tokens.c

BPRserver_LDADD = \
$(GLOBUS_GSS_NOTHR_LIBS) $(GLOBUS_SSL_NOTHR_LIBS)

noinst_HEADERS = blahpd.h classad_c_helper.h commands.h job_status.h resbuffer.h server.h console.h BPRcomm.h tokens.h

#bin_SCRIPTS = pbs_cancel.sh pbs_status.sh pbs_submit.sh lsf_cancel.sh lsf_status.sh lsf_submit.sh

#EXTRA_DIST = $(bin_SCRIPTS)
