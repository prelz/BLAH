# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.14 2011/09/22 08:12:13 pandreet Exp $ 
#
# File:           Makefile.am
#
# Author(s):      Francesco Prelz ($Author: pandreet $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 28-Nov-2005  Created 
#

wletcwmdir = $(sysconfdir)
wletcwm_DATA = blah.config.template blparser.conf.template

#install-data-hook:
#        /bin/cp $(DESTDIR)$(sysconfdir)/blah.config.template $(DESTDIR)$(sysconfdir)/blah.config


EXTRA_DIST = blah.config.template \
             blparser.conf.template \
             glite-ce-blparser \
             glite-ce-blahparser \
             glite-ce-blah \
             glite-ce-check-blparser 

bldir = $(sysconfdir)/$(initdir)
bl_SCRIPTS = glite-ce-blparser glite-ce-blahparser glite-ce-blah glite-ce-check-blparser

CLEANFILES =

MAINTAINERCLEANFILES = Makefile.in *~ *.*~

