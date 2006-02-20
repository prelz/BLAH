#
#       Copyright (c) 2004 on behalf of the EU EGEE Project:
#       The European Organization for Nuclear Research (CERN),
#       Istituto Nazionale di Fisica Nucleare (INFN), Italy
#       Datamat Spa, Italy
#       Centre National de la Recherche Scientifique (CNRS), France
#       CS Systeme d'Information (CSSI), France
#       Royal Institute of Technology, Center for Parallel Computers (KTH-PDC), Sweden
#       Universiteit van Amsterdam (UvA), Netherlands
#       University of Helsinki (UH.HIP), Finland
#       University of Bergen (UiB), Norway
#       Council for the Central Laboratory of the Research Councils (CCLRC), United Kingdom
#
#       Authors: Francesco Prelz <francesco.prelz@mi.infn.it>
#       Version info: $Id: configure.ac,v 1.29 2006/02/20 09:06:54 pandreet Exp $
#       Release: $Name:  $
#
#       Revision history:
#       $Log: configure.ac,v $
#       Revision 1.29  2006/02/20 09:06:54  pandreet
#       Incremented version number
#
#       Revision 1.28  2006/02/16 10:49:47  mezzadri
#       changed version number
#
#       Revision 1.27  2005/12/01 16:19:17  gfiorent
#       Fixed bug #14007, filtered "env" attribute in submission classad
#
#       Revision 1.26  2005/11/28 10:05:25  mezzadri
#       added config file for blah
#
#       Revision 1.25  2005/10/05 10:11:36  pandreet
#       Increased version number
#
#       Revision 1.24  2005/08/05 15:15:48  pandreet
#       Changed version to 1.5
#
#       Revision 1.23  2005/07/27 10:23:33  pandreet
#       Modified version number
#
#       Revision 1.22  2005/07/27 09:35:47  pandreet
#       Modified version number
#
#       Revision 1.21  2005/06/08 14:12:30  mezzadri
#       changed version from 1.2.2 to 1.2.4
#
#       Revision 1.20  2005/03/23 11:12:19  drebatto
#       Some more files to align...
#
#       Revision 1.14.2.4  2005/03/11 19:42:26  fprelz
#       Added support for the BLAH_JOB_REFRESH_PROXY command in blahpd.
#       Revised the batch system submit and status scripts.
#
#       Revision 1.14.2.3  2005/02/25 14:25:37  mezzadri
#       changed minor version number
#
#       Revision 1.14.2.2  2005/02/03 16:19:04  drebatto
#       Version number incremented
#
#       Revision 1.14.2.1  2005/02/01 13:54:26  drebatto
#       Aligned to MAIN
#
#       Revision 1.14  2004/12/09 13:59:13  mezzadri
#       changed version number
#
#       Revision 1.13  2004/11/25 14:53:34  mezzadri
#       changed version number
#
#       Revision 1.12  2004/11/16 15:45:12  eronchie
#       Increased version
#
#       Revision 1.11  2004/10/05 16:05:45  drebatto
#       Support for "gridtype" added.
#       Patched a bug preventing classad with both "Args" and "Queue" to be parsed correctly.
#
#       Revision 1.10  2004/09/22 11:44:20  pandreet
#       Runtime scripts in tarball and rpms
#
#       Revision 1.9  2004/09/22 09:16:56  glbuild
#       Removed AM_CONFIG_HEADER
#
#       Revision 1.8  2004/09/22 01:54:20  dimeglio
#       Added AM_CONFIG_HEADER
#
#       Revision 1.7  2004/09/22 00:15:23  dimeglio
#       Fixed missing parenthesis in AC_INIT
#       Added default prefix
#
#

AC_PREREQ(2.57)
AC_INIT([GLite CE blahp], [1.7.0])
AC_CONFIG_AUX_DIR([./project])
AM_INIT_AUTOMAKE([1.6.3 subdir-objects])
AC_CONFIG_SRCDIR([src/main.c])
                                                                                
#Environment.
WORKDIR=`pwd`
AC_SUBST(WORKDIR)
                                                                                                                          
dnl Checks for programs.
AC_PROG_AWK
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_LIBTOOL

dnl Checks for libraries.
dnl Replace `main' with a function in -lpthread:
AC_CHECK_LIB(pthread, main)

dnl Checks for header files.
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS(sys/time.h syslog.h)

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_PID_T
AC_HEADER_TIME

dnl Checks for library functions.
AC_CHECK_FUNCS(select socket strdup strerror bsearch vsnprintf)

AC_GLITE

have_classads=no
have_globus=no

AC_CLASSADS([], have_classads=yes, have_classads=no)
AC_GLOBUS([], have_globus=yes, have_globus=no)

AC_MSG_RESULT(["CLASSADS found $have_classads"])
AC_MSG_RESULT(["GLOBUS found $have_globus"])

dnl Temporarily built with no optimisation

CXXFLAGS="-g -O0"
CFLAGS="-g -O0"

AC_DEFINE(BLAHPD_BINDIR,"@bindir@")

# Configuration items
AC_PREFIX_DEFAULT([/opt/glite])
AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([config/Makefile])
AC_CONFIG_FILES([src/Makefile])
AC_CONFIG_FILES([src/scripts/Makefile])

AC_OUTPUT
