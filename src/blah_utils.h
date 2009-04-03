/*
#  File:     blah_utils.h
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#   30 Mar 2009 - Original release.
#
#  Description:
#   Utility functions for blah protocol
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/

#ifndef BLAHP_UTILS_INCLUDED
#define BLAHP_UTILS_INCLUDED

extern const char *blah_omem_msg;

char *make_message(const char *fmt, ...);
char *escape_spaces(const char *str);

#define BLAH_DYN_ALLOCATED(escstr) ((escstr) != blah_omem_msg && (escstr) != NULL)

#endif /* ifndef BLAHP_UTILS_INCLUDED */
