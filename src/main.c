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
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
/* #include <syslog.h> */
#include <mcheck.h>

#include "blahpd.h"
#include "server.h"
#include "console.h"

int
main(int argc, char *argv[])
{
    int fd_socket, read_socket;
    struct sockaddr_in recvs;
    struct protoent *prot_descr;
    struct sockaddr_in cli_addr;
    char client_ip[16];
    struct hostent *resolved_client;
    int addr_size = sizeof(cli_addr);
    fd_set readfs, masterfs;
    int retcod, status;
    int exit_program = 0;
    pid_t pid;
   
    /* 
    openlog("blahpd", LOG_PID, LOG_DAEMON);
    syslog(LOG_DAEMON | LOG_INFO, "Starting blah server (%s)", RCSID_VERSION);
    */
    
    serveConnection(0, "(stdin)");

    exit(0);
}
