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
#include <syslog.h>
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

    mtrace();
    
    openlog("blahpd", LOG_PID, LOG_DAEMON);
    syslog(LOG_DAEMON | LOG_INFO, "Starting blah server (%s)", RCSID_VERSION);
    
    printf("Starting BLAHP server...\n");
    printf("%s\n", RCSID_VERSION);

    recvs.sin_family = AF_INET;
    recvs.sin_addr.s_addr = htonl(INADDR_ANY);
    recvs.sin_port = htons(19999);

    prot_descr = getprotobyname("tcp");
    if (prot_descr == NULL)
    { 
        fprintf(stderr, "TCP protocol could not be found in /etc/protocols.\n");
        exit(1);
    }
    if ((fd_socket = socket(PF_INET,SOCK_STREAM, prot_descr->p_proto)) == -1)
    {
        fprintf(stderr, "Cannot create socket: %s\n", strerror(errno));
        exit(1);
    }
    if (bind(fd_socket,(struct sockaddr *)&recvs,sizeof(recvs)) == -1)
    {
        fprintf(stderr, "Cannot bind socket: %s\n", strerror(errno));
        exit(1);
    }
    if (listen(fd_socket,1) == -1)
    {
        fprintf(stderr, "Cannot listen from socket: %s\n", strerror(errno));
        exit(1);
    } else printf("Server up and listening on port 19999...\n");

    FD_ZERO(&masterfs);
    FD_SET(0, &masterfs);
    FD_SET(fd_socket, &masterfs);

    while (!exit_program)
    {
        printf("\nBLAHP Server > ");

        readfs = masterfs;

        fflush(stdout);
        if ((retcod = select(FD_SETSIZE, &readfs, (fd_set *) 0, (fd_set *) 0, (struct timeval *) 0)) < 0)
        {
            perror("Select error");
            close(fd_socket);
            exit(1);
        } 

        if (FD_ISSET(0, &readfs))
        {
            exit_program = processConsoleCommand();
        }

        if (FD_ISSET(fd_socket, &readfs))
        {
            if ((read_socket = accept(fd_socket, (struct sockaddr *)&cli_addr, &addr_size)) == -1)
            {
                fprintf(stderr,"\nCannot accept connection: %s\n", strerror(errno));
                exit(1);
            }
            sprintf(client_ip, "%s", inet_ntoa(cli_addr.sin_addr));
            printf("\nIncoming connection from %s\n", client_ip);
            while (waitpid(-1, &status, WNOHANG) > 0);
            pid = fork();
            if (pid < 0)
            {
                fprintf(stderr, "\nCannot fork connection manager: %s\n", strerror(errno));
                close(read_socket);
            }
            else if (pid >0)
            {
                printf("\nNew connection managed by child process %d\n", pid);
                close(read_socket);
            }
            else
            {
                close(fd_socket);
		syslog(LOG_DAEMON | LOG_INFO, "fork to serve connection from %s", client_ip);
                serveConnection(read_socket, client_ip);
            }
        }
    }

    printf("Server shutting down...\n");

    shutdown(fd_socket, SHUT_RDWR);
    close(fd_socket);
    printf("Socket closed\n");

    /* Shutdown of child processes */

    printf("Goodbye!\n");
    exit(0);
}
