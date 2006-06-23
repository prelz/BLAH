#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <popt.h>
#include <netdb.h>

#include "BLhelper.h"

#define MAX_LINE            100000
#define STR_CHARS           1000
#define CONN_TIMEOUT_SEC    0
#define CONN_TIMEOUT_MSEC   100000

#ifndef EOL      /* End of Line */
#define EOL '\0' /* 000   0  00 */
#endif

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

char     *progname = "BLClient";

int main(int argc, char *argv[]) {

    int       conn_s;
    struct    sockaddr_in servaddr;
    char      buffer[MAX_LINE];
    
    char      *address=NULL;
    int       port = 0;
    int       version=0;

    fd_set   wset;
    struct   timeval to;
    int      r,i;
    int opt;
    size_t optlen = sizeof(opt);

    struct hostent *hp;
    char *ipaddr;
     
    /*  Get command line arguments  */

    poptContext poptcon;	
    int rc;				
    struct poptOption poptopt[] = {
        { "server",    'a', POPT_ARG_STRING, &address,  0, "server address", "<dotted-quad ip address>" },
        { "port",      'p', POPT_ARG_INT,    &port,    0, "port",               "<port number>" },
        { "version",   'v', POPT_ARG_NONE,   &version, 0, "print version and exit",            NULL },
        POPT_AUTOHELP
        POPT_TABLEEND
    };
    
    poptcon = poptGetContext(NULL, argc, (const char **) argv, poptopt, 0);
       
    if((rc = poptGetNextOpt(poptcon)) != -1){
        fprintf(stderr,"%s: Invalid flag supplied.\n",progname);
	exit(EXIT_FAILURE);
    }

    if ( !port && !address && !version ) {
        poptPrintHelp(poptcon, stdout, 0);
        exit(EXIT_SUCCESS);
    }

    if ( version ) {
        printf("%s Version: %s\n",progname,VERSION);
        exit(EXIT_SUCCESS);
    } 

    if ( !port ) {
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
    
    if ( !address || (hp = gethostbyname(address)) == NULL ){
	fprintf(stderr, "%s: Invalid hostname.\n",progname);
	exit(EXIT_FAILURE);
    }
    
    if((ipaddr=malloc(STR_CHARS)) == 0){
        fprintf(stderr, "%s: Can't malloc ipaddr.\n",progname);
	exit(EXIT_FAILURE);
    }
    while ( hp -> h_addr_list[i] != NULL) {
        strcat(ipaddr,inet_ntoa( *( struct in_addr*)( hp -> h_addr_list[i])));
        i++;
    }

    if ( inet_aton(ipaddr, &servaddr.sin_addr) <= 0 ) {
	fprintf(stderr, "%s: Invalid remote IP address.\n",progname);
	exit(EXIT_FAILURE);
    }
   
    if((r = fcntl(conn_s, F_GETFL, NULL)) < 0) {
	fprintf(stderr, "%s: Error in getfl for socket.\n",progname);
	exit(EXIT_FAILURE);
    }
 
    r |= O_NONBLOCK;
 
    if(fcntl(conn_s, F_SETFL, r) < 0) {
	fprintf(stderr, "%s: Error in setfl for socket.\n",progname);
	exit(EXIT_FAILURE);
    }
        
    if ( connect(conn_s, (struct sockaddr *) &servaddr, sizeof(servaddr) ) < 0 ) {
        if(errno == EINPROGRESS) {
            to.tv_sec  = CONN_TIMEOUT_SEC;
            to.tv_usec = CONN_TIMEOUT_MSEC;
            
            FD_ZERO(&wset);
            FD_SET(conn_s, &wset);
	    
            r = select(1 + conn_s, NULL, &wset, NULL, &to);
            
            if(r < 0) {
	        exit(EXIT_FAILURE);
            } else if(r == 0) {
                errno = ECONNREFUSED;
                exit(EXIT_FAILURE);
            }
        } else {
            exit(EXIT_FAILURE);
        }
    }    

    if(FD_ISSET(conn_s, &wset)) {
         
       if(getsockopt(conn_s, SOL_SOCKET, SO_ERROR, (void *) &opt, &optlen) < 0) {
	    fprintf(stderr, "%s: Error in getsockopt for socket.\n",progname);
            exit(EXIT_FAILURE);
       }
       
       if(opt) {
            errno = opt;
            exit(EXIT_FAILURE);
       }    
       
       if((r = fcntl(conn_s, F_GETFL, NULL)) < 0) {
	     fprintf(stderr, "%s: Error in getfl for socket.\n",progname);
	     exit(EXIT_FAILURE);
       }
    
       r &= (~O_NONBLOCK);

       if(fcntl(conn_s, F_SETFL, r) < 0) {
	   fprintf(stderr, "%s: Error in setfl for socket.\n",progname);
           exit(EXIT_FAILURE);
       }
    
       fgets(buffer, MAX_LINE, stdin);

       Writeline(conn_s, buffer, strlen(buffer));
       Readline(conn_s, buffer, MAX_LINE-1);

       printf("%s", buffer);
    } else {
        exit(EXIT_FAILURE);
    }

    /*  Close the connected socket  */

    if ( close(conn_s) < 0 ) {
       fprintf(stderr, "%s: Error calling close()\n",progname);
       exit(EXIT_FAILURE);
    }

    exit(EXIT_SUCCESS);
}
