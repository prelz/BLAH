#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "BLhelper.h"

/*  Global constants  */

#define MAX_LINE           (100000)

char     *progname = "BLClient";

/*  Function declarations  */

int ParseCmdLine(int argc, char *argv[], char **szAddress, char **szPort);
void print_usage();


int main(int argc, char *argv[]) {

    int       conn_s;                /*  connection socket         */
    short int port;                  /*  port number               */
    struct    sockaddr_in servaddr;  /*  socket address structure  */
    char      buffer[MAX_LINE];      /*  character buffer          */
    char     *szAddress;             /*  Holds remote IP address   */
    char     *szPort;                /*  Holds remote port         */
    char     *endptr;                /*  for strtol()              */


    /*  Get command line arguments  */

    ParseCmdLine(argc, argv, &szAddress, &szPort);

    /*  Set the remote port  */
    
    if(szPort !=NULL){
     port = strtol(szPort, &endptr, 0);
     if ( *endptr ) {
         fprintf(stderr,"%s: Invalid port supplied.\n",progname);
	 exit(EXIT_FAILURE);
     }
    }else{
      fprintf(stderr,"%s: Invalid port supplied.\n",progname);
      exit(EXIT_FAILURE);
    }
   
    if ( (conn_s = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
	fprintf(stderr, "%s: Error creating listening socket.\n",progname);
	exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);


    /*  Set the remote IP address  */

    if ( !szAddress || inet_aton(szAddress, &servaddr.sin_addr) <= 0 ) {
	fprintf(stderr, "%s: Invalid remote IP address.\n",progname);
	exit(EXIT_FAILURE);
    }

    if ( connect(conn_s, (struct sockaddr *) &servaddr, sizeof(servaddr) ) < 0 ) {
	fprintf(stderr, "%s: Error calling connect().\n",progname);
	exit(EXIT_FAILURE);
    }

    fgets(buffer, MAX_LINE, stdin);

    Writeline(conn_s, buffer, strlen(buffer));
    Readline(conn_s, buffer, MAX_LINE-1);

    printf("%s", buffer);

   /*  Close the connected socket  */

   if ( close(conn_s) < 0 ) {
     fprintf(stderr, "%s: Error calling close()\n",progname);
     exit(EXIT_FAILURE);
   }


    return EXIT_SUCCESS;
}

void print_usage(){

     fprintf(stderr,"Usage:\n");
     fprintf(stderr,"%s -a (remote IP) -p (remote port)\n",progname);
     exit(EXIT_SUCCESS);

}

int ParseCmdLine(int argc, char *argv[], char **szAddress, char **szPort) {

    int n = 1;

    if(argc < 3){
      print_usage();
    }
    
    *szAddress=NULL;
    *szPort=NULL;

    while ( n < argc ) {
	if ( !strncmp(argv[n], "-a", 2) ) {
	    *szAddress = argv[++n];
	}
	else if ( !strncmp(argv[n], "-p", 2) ) {
	    *szPort = argv[++n];
	}
	else if ( !strncmp(argv[n], "-h", 2) ) {
            print_usage();
	}
	++n;
    }
    
    return 0;
}

