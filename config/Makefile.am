# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.13 2011/05/18 13:12:55 mezzadri Exp $ 
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
wletcwm_DATA = blah.config.template blparser.conf.template

#install-data-hook:
#        /bin/cp $(DESTDIR)$(sysconfdir)/blah.config.template $(DESTDIR)$(sysconfdir)/blah.config


EXTRA_DIST = blah.config.template \
             blparser.conf.template \
             glite-ce-blparser \
             glite-ce-blahparser \
             glite-ce-blah \
             glite-ce-check-blparser 

bldir = $(sysconfdir)/init.d
bl_SCRIPTS = glite-ce-blparser glite-ce-blahparser glite-ce-blah glite-ce-check-blparser

CLEANFILES =

MAINTAINERCLEANFILES = Makefile.in *~ *.*~

