#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <globus_gss_assist.h>
#include <globus_gsi_credential.h>
#include <globus_gsi_proxy.h>
#include <globus_gsi_cert_utils.h>
#include "tokens.h"
#include "BPRcomm.h"
#include "BLhelper.h"

/*  Global constants  */

#define MAX_LINE           (100000)

char     *progname = "BAClient";

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
    
    
    OM_uint32	    major_status;
    OM_uint32	    minor_status;
    int 	    token_status = 0;
    gss_cred_id_t   credential_handle = GSS_C_NO_CREDENTIAL;
    OM_uint32	    ret_flags = 0;
    gss_ctx_id_t    context_handle = GSS_C_NO_CONTEXT;

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
	
        /* Acquire GSS credential */
        if ((credential_handle = acquire_cred(GSS_C_INITIATE)) == GSS_C_NO_CREDENTIAL)
        {
                fprintf(stderr,"%s: Unable to acquire credentials, exiting...\n",progname);
                exit(EXIT_FAILURE);
        }


    if ( (conn_s = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
        fprintf(stderr,"%s: Error creating listening socket.\n",progname);
	exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);


    /*  Set the remote IP address  */

    if ( !szAddress || inet_aton(szAddress, &servaddr.sin_addr) <= 0 ) {
        fprintf(stderr,"%s: Invalid remote IP address.\n",progname);
	exit(EXIT_FAILURE);
    }
    
    if ( connect(conn_s, (struct sockaddr *) &servaddr, sizeof(servaddr) ) < 0 ) {
        fprintf(stderr,"%s: Error calling connect().\n",progname);
	exit(EXIT_FAILURE);
    }
    
     if ((context_handle = initiate_context(credential_handle, "GSI-NO-TARGET", conn_s)) == GSS_C_NO_CONTEXT)
     {
             fprintf(stderr,"%s: Cannot initiate security context...\n",progname);
   	     exit(EXIT_FAILURE);
     }

     if (verify_context(context_handle))
     {
             fprintf(stderr,"%s: Error: wrong server certificate.\n",progname);
    	     exit(EXIT_FAILURE);
     }


    fgets(buffer, MAX_LINE, stdin);
    
    Writeline(conn_s, buffer, strlen(buffer));
    Readline(conn_s, buffer, MAX_LINE-1);


    printf("%s", buffer);

   /*  Close the connected socket  */

   if ( close(conn_s) < 0 ) {
     fprintf(stderr,"%s:Error calling close()\n",progname);
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

