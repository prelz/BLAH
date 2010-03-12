# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.45 2010/03/12 09:23:34 mezzadri Exp $ 
#
# File:           Makefile.am
#
# Author(s):      Francesco Prelz ($Author: mezzadri $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 16-Apr-2004  Created 
#

INCLUDES = -I. $(CLASSAD_CFLAGS) $(GLOBUS_NOTHR_CFLAGS)

SUBDIRS = scripts

if HAVE_GLOBUS
GLOBUS_EXECS = BPRclient BPRserver
else
GLOBUS_EXECS =
endif

bin_PROGRAMS = blahpd blahpd_daemon BLClient BLParserLSF BLParserPBS test_condor_logger blah_job_registry_add blah_job_registry_lkup blah_job_registry_scan_by_subject BUpdaterCondor BNotifier BUpdaterLSF BUpdaterPBS BUpdaterSGE $(GLOBUS_EXECS)  blparser_master blah_check_config
noinst_PROGRAMS = test_job_registry_create test_job_registry_purge test_job_registry_update test_job_registry_access

common_sources = console.c job_status.c resbuffer.c server.c commands.c classad_binary_op_unwind.C classad_c_helper.C proxy_hashcontainer.c config.c job_registry.c Bfunctions.c blah_utils.c env_helper.c mapped_exec.c md5.c 

blahpd_SOURCES = main.c $(common_sources)

blahpd_daemon_SOURCES = main_daemon.c $(common_sources)

blahpd_LDADD = $(CLASSAD_LIBS)

blahpd_daemon_LDADD = $(blahpd_LDADD)

BPRclient_SOURCES = BPRclient.c BPRcomm.c tokens.c

BPRclient_LDADD = \
$(GLOBUS_GSS_NOTHR_LIBS) $(GLOBUS_SSL_NOTHR_LIBS)

BPRserver_SOURCES = BPRserver.c BPRcomm.c tokens.c

BPRserver_LDFLAGS = -static

BPRserver_LDADD = \
$(GLOBUS_GSS_NOTHR_LIBS) $(GLOBUS_SSL_NOTHR_LIBS)

BLClient_SOURCES = BLClient.c blah_utils.c BLfunctions.c

BLClient_LDADD =

BLParserLSF_SOURCES = BLParserLSF.c blah_utils.c BLfunctions.c

BLParserLSF_LDADD = 

BLParserPBS_SOURCES = BLParserPBS.c blah_utils.c BLfunctions.c

BLParserPBS_LDADD =

test_condor_logger_SOURCES = test_condor_create_log.c condor_create_log.c config.c blah_utils.c
test_condor_logger_CFLAGS = $(AM_CFLAGS)

blah_job_registry_add_SOURCES = blah_job_registry_add.c job_registry.c md5.c config.c
blah_job_registry_add_CFLAGS = $(AM_CFLAGS)

blah_job_registry_lkup_SOURCES = blah_job_registry_lkup.c job_registry.c md5.c config.c
blah_job_registry_lkup_CFLAGS = $(AM_CFLAGS)

blah_job_registry_scan_by_subject_SOURCES = blah_job_registry_scan_by_subject.c classad_c_helper.C classad_binary_op_unwind.C job_registry.c md5.c config.c
blah_job_registry_scan_by_subject_LDADD = $(CLASSAD_LIBS)
blah_job_registry_scan_by_subject_CFLAGS = $(AM_CFLAGS)

test_job_registry_purge_SOURCES = test_job_registry_purge.c job_registry.c md5.c
test_job_registry_purge_CFLAGS = $(AM_CFLAGS)

test_job_registry_create_SOURCES = test_job_registry_create.c job_registry.c md5.c
test_job_registry_create_CFLAGS = $(AM_CFLAGS)

test_job_registry_update_SOURCES = test_job_registry_update.c job_registry.c md5.c
test_job_registry_update_CFLAGS = $(AM_CFLAGS)

test_job_registry_access_SOURCES = test_job_registry_access.c job_registry.c md5.c
test_job_registry_access_CFLAGS = $(AM_CFLAGS)

BUpdaterCondor_SOURCES = BUpdaterCondor.c Bfunctions.c job_registry.c md5.c config.c blah_utils.c
BUpdaterCondor_LDADD = -lpthread

BNotifier_SOURCES = BNotifier.c Bfunctions.c job_registry.c md5.c config.c blah_utils.c
BNotifier_LDADD = -lpthread

BUpdaterLSF_SOURCES = BUpdaterLSF.c Bfunctions.c job_registry.c md5.c config.c blah_utils.c
BUpdaterLSF_LDADD = -lpthread

BUpdaterPBS_SOURCES = BUpdaterPBS.c Bfunctions.c job_registry.c md5.c config.c blah_utils.c
BUpdaterPBS_LDADD = -lpthread -lm

BUpdaterSGE_SOURCES = BUpdaterSGE.c Bfunctions.c job_registry.c md5.c config.c blah_utils.c
BUpdaterSGE_LDADD = -lpthread

blparser_master_SOURCES = blparser_master.c config.c blah_utils.c
blparser_master_LDADD = 

blah_check_config_SOURCES = blah_check_config.c Bfunctions.c config.c blah_utils.c
blah_check_config_LDADD =

noinst_HEADERS = blahpd.h classad_binary_op_unwind.h classad_c_helper.h commands.h job_status.h resbuffer.h server.h console.h BPRcomm.h tokens.h BLParserPBS.h BLParserLSF.h condor_create_log.h proxy_hashcontainer.h job_registry.h md5.h config.h BUpdaterCondor.h Bfunctions.h BNotifier.h BUpdaterLSF.h BUpdaterPBS.h BUpdaterSGE.h blah_utils.h env_helper.h mapped_exec.h blah_check_config.h

#bin_SCRIPTS = pbs_cancel.sh pbs_status.sh pbs_submit.sh lsf_cancel.sh lsf_status.sh lsf_submit.sh

#EXTRA_DIST = $(bin_SCRIPTS)
