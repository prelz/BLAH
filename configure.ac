dnl Process this file with autoconf to produce a configure script.
AC_INIT(src/main.c)
                                                                                
# Initialize automake
AM_INIT_AUTOMAKE(blahpd,0.12)

dnl Checks for programs.
AC_PROG_AWK
AC_PROG_CC

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

AC_PROG_CXX

AC_PROG_INSTALL

dnl Temporarily built with no optimisation

CXXFLAGS="-g -O0"
CFLAGS="-g -O0"

dnl Checks for a functional classad library

AC_ARG_WITH(classad,
 [  --with-classad=PATH     Define location of classad library installation],
 [ CXXFLAGS="$CXXFLAGS -I${withval}/include"
   LDFLAGS="$LDFLAGS -L${withval}/lib" ] )

AC_CACHE_CHECK([whether a program using classads can be built],
               [ac_cv_classad_program_builds],
 [
  ac_cv_classad_program_builds="no";
  ac_cv_classad_program_builds_no_namespace="no";
  ac_cv_classad_program_builds_with_namespace="no";
  ac_classad_save_LIBS="$LIBS"
  ac_classad_save_CXXFLAGS="$CXXFLAGS"
  LIBS="$LIBS -lclassad"
  AC_LANG_SAVE
  AC_LANG_CPLUSPLUS
  AC_TRY_LINK([ #include <classad_distribution.h> ],
            [ 
ClassAd ad; 
ClassAdParser parser; 
            ],
            ac_cv_classad_program_builds_no_namespace="yes",
            ac_cv_classad_program_builds_no_namespace="no")
  CXXFLAGS="$CXXFLAGS -DWANT_NAMESPACES"
  AC_TRY_LINK([ #include <classad_distribution.h> ],
            [ 
using namespace classad;
ClassAd ad; 
ClassAdParser parser; 
            ],
            ac_cv_classad_program_builds_with_namespace="yes",
            ac_cv_classad_program_builds_with_namespace="no")

  if test $ac_cv_classad_program_builds_with_namespace = yes || test $ac_cv_classad_program_builds_no_namespace = yes; then
    ac_cv_classad_program_builds="yes";
  fi
  LIBS="$ac_classad_save_LIBS"
  CXXFLAGS="$ac_classad_save_CXXFLAGS"
  AC_LANG_RESTORE
 ] )

if test $ac_cv_classad_program_builds = no; then
  AC_MSG_ERROR([
The Condor classad library is required to be installed. 
The install path can be specified with --with-classad=PATH]) 
fi

if test $ac_cv_classad_program_builds_with_namespace = yes && test $ac_cv_classad_program_builds_no_namespace = no; then
  CXXFLAGS="$CXXFLAGS -DWANT_NAMESPACES"
fi

AC_DEFINE(BLAHPD_INSTALL,"@prefix@")
AC_DEFINE(BLAHPD_BINDIR,"@bindir@")

AC_OUTPUT(Makefile)
