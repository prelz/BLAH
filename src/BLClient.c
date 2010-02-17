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
#include <getopt.h>
#include <netdb.h>

#define MAX_LINE            100000
#define STR_CHARS           1000
#define CONN_TIMEOUT_SEC    0
#define CONN_TIMEOUT_MSEC   100000

#ifndef VERSION
#define VERSION            "1.8.0"
#endif

int usage();
int short_usage();

char     *progname = "BLClient";

int
main(int argc, char *argv[])
{

	int       conn_s;
	char      buffer[MAX_LINE];
    
	char      *address=NULL;
	int       port = 0;
	int       version=0;
	char      ainfo_port_string[16];
	struct    addrinfo ai_req, *ai_ans, *cur_ans;
	int c;				

        static int help;
        static int short_help;

	fd_set   wset;
	struct   timeval to;
	int      r,i;
	int opt;
	socklen_t optlen = sizeof(opt);

	struct hostent *hp;
	char *ipaddr;

	char      *cp;

	while (1) {
		static struct option long_options[] =
		{
		{"help",      no_argument,     &help,       1},
		{"usage",     no_argument,     &short_help, 1},
		{"version",   no_argument,             0, 'v'},
		{"port",      required_argument,       0, 'p'},
		{"server",    required_argument,       0, 'a'},
		{0, 0, 0, 0}
		};

		int option_index = 0;
     
		c = getopt_long (argc, argv, "vp:a:",long_options, &option_index);
     
		if (c == -1){
			break;
		}
     
		switch (c)
		{

		case 0:
		if (long_options[option_index].flag != 0){
			break;
		}
     
		case 'v':
			version=1;
			break;
	       
		case 'p':
			port=atoi(optarg);
			break;
	       
		case 'a':
			address=strdup(optarg);
			break;
    	       
		case '?':
			break;
     
		default:
			abort ();
		}
	}
	
	if(help){
		usage();
	}
	 
	if(short_help){
		short_usage();
	}
	if ( !port && !address && !version ) {
		usage();
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
     
	ai_req.ai_flags = 0;
	ai_req.ai_family = PF_UNSPEC;
	ai_req.ai_socktype = SOCK_STREAM;
	ai_req.ai_protocol = 0; /* Any stream protocol is OK */

	sprintf(ainfo_port_string,"%5d",port);

	if (getaddrinfo(address, ainfo_port_string, &ai_req, &ai_ans) != 0) {
		printf("%s: unknown host %s", progname, address);
		exit(EXIT_FAILURE);
	}

	/* Try all found protocols and addresses */
	for (cur_ans = ai_ans; cur_ans != NULL; cur_ans = cur_ans->ai_next)
	{
		/* Create the socket everytime (cannot be reused once closed) */
		if ((conn_s = socket(cur_ans->ai_family, 
				     cur_ans->ai_socktype,
				     cur_ans->ai_protocol)) == -1)
		{
			continue;
		}

		if((r = fcntl(conn_s, F_GETFL, NULL)) < 0) {
			fprintf(stderr, "%s: Error in getfl for socket.\n",progname);
			close(conn_s);
			conn_s = -1;
			continue;
		}
 
		r |= O_NONBLOCK;
 
		if(fcntl(conn_s, F_SETFL, r) < 0) {
			fprintf(stderr, "%s: Error in setfl for socket.\n",progname);
			close(conn_s);
			conn_s = -1;
			continue;
		}
        
		if ( connect(conn_s, cur_ans->ai_addr, cur_ans->ai_addrlen ) < 0 ) {
			if(errno == EINPROGRESS) break;
		} else break;   
		close(conn_s);
	}
	freeaddrinfo(ai_ans);

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
    
		fgets(buffer, MAX_LINE-1, stdin);

		if(strstr(buffer,"/")==NULL){
			if ((cp = strrchr (buffer, '\n')) != NULL){
				*cp = '\0';
			}
			strcat(buffer,"/\n");
		}
       
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

int 
usage()
{
	printf("Usage: BLClient [OPTION...]\n");
	printf("  -a, --server=<dotted-quad ip address>     server address\n");
	printf("  -p, --port=<port number>                  port\n");
	printf("  -v, --version                             print version and exit\n");
	printf("\n");
	printf("Help options:\n");
	printf("  -?, --help                                Show this help message\n");
	printf("  --usage                                   Display brief usage message\n");
	exit(EXIT_SUCCESS);
}

int 
short_usage()
{
	printf("Usage: BLClient [-v?] [-a|--server <dotted-quad ip address>]\n");
	printf("        [-p|--port <port number>] [-v|--version] [-?|--help] [--usage]\n");
	exit(EXIT_SUCCESS);
}
