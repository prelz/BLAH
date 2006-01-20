# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.5 2006/01/20 09:59:27 mezzadri Exp $ 
#
# File:           Makefile.am
#
# Author(s):      Francesco Prelz ($Author: mezzadri $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 28-Nov-2005  Created 
#

wletcwmdir = $(sysconfdir)
wletcwm_DATA = blah.config blparser.conf baparser.conf

install-data-hook:
	/bin/cp $(DESTDIR)$(sysconfdir)/blah.config $(DESTDIR)$(sysconfdir)/blah.config.template

EXTRA_DIST = blah.config \
             blparser.conf \
             baparser.conf \
             glite-ce-blparser \
             glite-ce-baparser

bldir = $(sysconfdir)/init.d
bl_SCRIPTS = glite-ce-blparser glite-ce-baparser

CLEANFILES =

MAINTAINERCLEANFILES = Makefile.in *~ *.*~

