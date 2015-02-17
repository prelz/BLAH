#ifndef __BLAH_VERSION_H__
#define __BLAH_VERSION_H__

/* Protocol version */
#ifndef BLAH_PROTOCOL
#define BLAH_PROTOCOL "1.8.0"
#endif

/* The following are fallback values, the proper definitions are set by CMake.
 * No need to change anything as long as compilation goes through CMake.
 */

/* Daemon version */
#ifndef BLAH_VERSION
#define BLAH_VERSION "1.21.0"
#endif

/* Commit date */
#ifndef BLAH_BUILD_TIMESTAMP
#define BLAH_BUILD_TIMESTAMP "Mar 31 2008"
#endif

/* VERSION command result */
#define GAHP_VERSION_STRING "$GahpVersion: " BLAH_PROTOCOL " "      \
                            BLAH_BUILD_TIMESTAMP " "                \
                            "blahpd\\ v." BLAH_VERSION "\\ (INFN) " \
                            "$"

/* For internal usage (eg. log files) */
#define BLAH_VERSION_STRING "v." BLAH_VERSION "(" BLAH_BUILD_TIMESTAMP ")"

#endif /*ifndef __BLAH_VERSION_H__*/
