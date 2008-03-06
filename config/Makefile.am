# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.8 2008/03/06 18:15:01 fprelz Exp $ 
#
# File:           Makefile.am
#
# Author(s):      Francesco Prelz ($Author: fprelz $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 28-Nov-2005  Created 
#

wletcwmdir = $(sysconfdir)
wletcwm_DATA = blah.config.template blparser.conf.template baparser.conf.template

#install-data-hook:
#        /bin/cp $(DESTDIR)$(sysconfdir)/blah.config.template $(DESTDIR)$(sysconfdir)/blah.config


EXTRA_DIST = blah.config.template \
             blparser.conf.template \
             baparser.conf.template \
             glite-ce-blparser \
             glite-ce-baparser

bldir = $(sysconfdir)/init.d
bl_SCRIPTS = glite-ce-blparser glite-ce-baparser

CLEANFILES =

MAINTAINERCLEANFILES = Makefile.in *~ *.*~

