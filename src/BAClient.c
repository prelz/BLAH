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
#include <popt.h>
#include <globus_gss_assist.h>
#include <globus_gsi_credential.h>
#include <globus_gsi_proxy.h>
#include <globus_gsi_cert_utils.h>
#include "tokens.h"
#include "BPRcomm.h"
#include "BLhelper.h"

#define MAX_LINE           100000
#ifndef VERSION
#define VERSION            "1.8.0"
#endif

char     *progname = "BAClient";

int main(int argc, char *argv[]) {

    int       conn_s;
    struct    sockaddr_in servaddr;
    char      buffer[MAX_LINE];
    
    char      *address=NULL;
    int       port = 0;
    int       version=0;

    fd_set   wset;
    struct   timeval to;
    int      r;
    int opt;
    size_t optlen = sizeof(opt);
    
    OM_uint32	    major_status;
    OM_uint32	    minor_status;
    int 	    token_status = 0;
    gss_cred_id_t   credential_handle = GSS_C_NO_CREDENTIAL;
    OM_uint32	    ret_flags = 0;
    gss_ctx_id_t    context_handle = GSS_C_NO_CONTEXT;

    /*  Get command line arguments  */

    poptContext poptcon;
    int rc;
    struct poptOption poptopt[] = {
        { "server",    'a', POPT_ARG_STRING, &address, 0, "server address", "<dotted-quad ip address>" },
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

    if ( version ) {
        printf("%s Version: %s\n",progname,VERSION);
        exit(EXIT_SUCCESS);
    }

    if ( !port ) {
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

    if ( !address || inet_aton(address, &servaddr.sin_addr) <= 0 ) {
        fprintf(stderr, "%s: Invalid remote IP address.\n",progname);
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


    exit(EXIT_SUCCESS);
}

