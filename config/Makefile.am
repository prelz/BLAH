# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: Makefile.am,v 1.1 2005/11/28 10:04:48 mezzadri Exp $ 
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
wletcwm_DATA = blah.config 

install-data-hook:
	/bin/cp $(DESTDIR)$(sysconfdir)/blah.config $(DESTDIR)$(sysconfdir)/blah.config.template

EXTRA_DIST = blah.config

CLEANFILES =

MAINTAINERCLEANFILES = Makefile.in *~ *.*~

