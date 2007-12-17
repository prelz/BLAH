# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.21 2007/12/17 09:40:45 fprelz Exp $ 
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

bin_PROGRAMS = blahpd blahpd_daemon BPRclient BPRserver BLClient BLParserLSF BLParserPBS BAParser BAClient BDlogger test_condor_logger blah_job_registry_add blah_job_registry_lkup
noinst_PROGRAMS = test_job_registry_create test_job_registry_purge test_job_registry_update test_job_registry_access

common_sources = console.c job_status.c resbuffer.c server.c commands.c classad_binary_op_unwind.C classad_c_helper.C mtsafe_popen.c proxy_hashcontainer.c config.c job_registry.c

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

test_condor_logger_SOURCES = test_condor_create_log.c condor_create_log.c config.c
test_condor_logger_CFLAGS = $(AM_CFLAGS)

blah_job_registry_add_SOURCES = blah_job_registry_add.c job_registry.c config.c
blah_job_registry_add_CFLAGS = $(AM_CFLAGS)

blah_job_registry_lkup_SOURCES = blah_job_registry_lkup.c job_registry.c config.c
blah_job_registry_lkup_CFLAGS = $(AM_CFLAGS)

test_job_registry_purge_SOURCES = test_job_registry_purge.c job_registry.c
test_job_registry_purge_CFLAGS = $(AM_CFLAGS)

test_job_registry_create_SOURCES = test_job_registry_create.c job_registry.c
test_job_registry_create_CFLAGS = $(AM_CFLAGS)

test_job_registry_update_SOURCES = test_job_registry_update.c job_registry.c
test_job_registry_update_CFLAGS = $(AM_CFLAGS)

test_job_registry_access_SOURCES = test_job_registry_access.c job_registry.c
test_job_registry_access_CFLAGS = $(AM_CFLAGS)

noinst_HEADERS = blahpd.h classad_binary_op_unwind.h classad_c_helper.h commands.h job_status.h resbuffer.h server.h console.h mtsafe_popen.h BPRcomm.h tokens.h BLParserPBS.h BLParserLSF.h BLhelper.h BAParser.h condor_create_log.h proxy_hashcontainer.h job_registry.h config.h

#bin_SCRIPTS = pbs_cancel.sh pbs_status.sh pbs_submit.sh lsf_cancel.sh lsf_status.sh lsf_submit.sh

#EXTRA_DIST = $(bin_SCRIPTS)
