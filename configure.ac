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
#       Version info: $Id: configure.ac,v 1.10 2004/09/22 11:44:20 pandreet Exp $
#       Release: $Name:  $
#
#       Revision history:
#       $Log: configure.ac,v $
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
AC_INIT([GLite CE blahp], [0.1.0])
AC_CONFIG_AUX_DIR([./project])
AM_INIT_AUTOMAKE([1.6.3 subdir-objects])
AC_CONFIG_SRCDIR([src/main.c])
                                                                                
#Environment.
WORKDIR=`pwd`
AC_SUBST(WORKDIR)
                                                                                                                          
dnl Checks for programs.
AC_PROG_AWK
AC_PROG_CC
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

AC_CLASSADS([], have_classads=yes, have_classads=no)

dnl Temporarily built with no optimisation

CXXFLAGS="-g -O0"
CFLAGS="-g -O0"

AC_DEFINE(BLAHPD_BINDIR,"@bindir@")

# Configuration items
AC_PREFIX_DEFAULT([/opt/glite])
AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([src/Makefile])
AC_CONFIG_FILES([src/scripts/Makefile])

AC_OUTPUT
