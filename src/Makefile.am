# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.14 2007/08/30 09:44:14 fprelz Exp $ 
#
# File:           Makefile.am
#
# Author(s):      Francesco Prelz ($Author: fprelz $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 16-Apr-2004  Created 
#

INCLUDES = -I. $(CLASSAD_CFLAGS) $(GLOBUS_NOTHR_CFLAGS)

SUBDIRS = scripts

bin_PROGRAMS = blahpd blahpd_daemon BPRclient BPRserver BLClient BLParserLSF BLParserPBS BAParser BAClient BDlogger test_condor_logger

common_sources = console.c job_status.c resbuffer.c server.c commands.c classad_binary_op_unwind.C classad_c_helper.C mtsafe_popen.c proxy_hashcontainer.c

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

BLClient_SOURCES = BLClient.c BLhelper.c

BLClient_LDADD = -lpopt

BLParserLSF_SOURCES = BLParserLSF.c

BLParserLSF_LDADD = -lpopt

BLParserPBS_SOURCES = BLParserPBS.c

BLParserPBS_LDADD = -lpopt

BAParser_SOURCES = BAParser.c BPRcomm.c tokens.c

BAParser_LDADD = -lpopt \
$(GLOBUS_GSS_NOTHR_LIBS) $(GLOBUS_SSL_NOTHR_LIBS)

BAClient_SOURCES = BAClient.c BPRcomm.c tokens.c BLhelper.c

BAClient_LDADD = -lpopt \
$(GLOBUS_GSS_NOTHR_LIBS) $(GLOBUS_SSL_NOTHR_LIBS)

BDlogger_SOURCES = BDlogger.c

BDlogger_LDADD =

test_condor_logger_SOURCES = test_condor_create_log.c condor_create_log.c

test_condor_logger_LDADD =

test_condor_logger_CFLAGS = $(AM_CFLAGS)

lib_LTLIBRARIES = libglite_ce_condor_logger.la 

libglite_ce_condor_logger_la_SOURCES = condor_create_log.c 


noinst_HEADERS = blahpd.h classad_binary_op_unwind.h classad_c_helper.h commands.h job_status.h resbuffer.h server.h console.h mtsafe_popen.h BPRcomm.h tokens.h BLParserPBS.h BLParserLSF.h BLhelper.h BAParser.h

#bin_SCRIPTS = pbs_cancel.sh pbs_status.sh pbs_submit.sh lsf_cancel.sh lsf_status.sh lsf_submit.sh

#EXTRA_DIST = $(bin_SCRIPTS)
