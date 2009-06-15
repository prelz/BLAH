/*
#  File:     main.c
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#   20 Mar 2004: Original release
#
#  Description:
#   Open a sochet and listen for connection; fork new
#   processes to serve incoming connections.
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <mcheck.h>

#include "blahpd.h"
#include "server.h"
#include "console.h"

extern int synchronous_termination;
/* #define MTRACE_ON */

int
main(int argc, char *argv[])
{
    int fd_socket, read_socket;
    fd_set readfs, masterfs;
    int retcod, status;
    int exit_program = 0;
    pid_t pid;
#ifdef MTRACE_ON
    char mtrace_log[2048];
#endif
   
#ifdef MTRACE_ON
    sprintf(mtrace_log, "mtrace_%d.log", getpid());
    setenv("MALLOC_TRACE", mtrace_log, 1);
    mtrace();
#endif

    if ((argc > 1) && (strncmp(argv[1],"-s",2) == 0))
        synchronous_termination = TRUE; 

    serveConnection(0, "(stdin)");

    exit(0);
}
