dnl Process this file with autoconf to produce a configure script.
AC_PREREQ(2.57)
AC_INIT([GLite CE blahp], [0.1.0]
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

AC_OUTPUT(Makefile)
AC_OUTPUT(src/Makefile)

