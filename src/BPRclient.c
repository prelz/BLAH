#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
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

char* chop(char* long_id);
char* pre_chop(char* long_id);

int
main(int argc, char **argv)
{
	int fd_socket;
	struct sockaddr_in localAddr, servAddr;
	int server_port;
	struct timeval tv;
	struct protoent *prot_descr;
	struct sockaddr_in cli_addr;
	char client_ip[16];
	struct hostent *resolved_client;
	int retcod, status;
	int proxy_file;
	char *proxy_filename;
	char *jobId;
	char *workernode;
	struct stat proxy_stat;
	char buffer[8192];
	char *message;
	char *globus_location;

	OM_uint32	major_status;
	OM_uint32	minor_status;
	int		token_status = 0;
	gss_cred_id_t	credential_handle = GSS_C_NO_CREDENTIAL;
	OM_uint32	ret_flags = 0;
	gss_ctx_id_t	context_handle = GSS_C_NO_CONTEXT;

	if (argc < 2)
	{
		printf("%s: missing proxy filename", argv[0]);
		exit(1);
	}
	proxy_filename = argv[1];

	if (argc < 3)
	{
		printf("%s: missing job name", argv[0]);
		exit(1);
	}
	jobId = argv[2];

	if (argc < 4)
	{
		printf("%s: missing worker node", argv[0]);
		exit(1);
	}
	workernode = argv[3];

	setenv("X509_USER_PROXY", proxy_filename, 1);

	/* Acquire GSS credential */
	if ((credential_handle = acquire_cred(GSS_C_INITIATE)) == GSS_C_NO_CREDENTIAL)
	{
		printf("%s: unable to acquire credentials", argv[0]);
		exit(2);
	}

	resolved_client = gethostbyname(workernode);
	if (resolved_client == NULL) {
		printf("%s: unknown host %s", argv[0], workernode);
		exit(4);
	}

	servAddr.sin_family = resolved_client->h_addrtype;
	memcpy((char *) &servAddr.sin_addr.s_addr, resolved_client->h_addr_list[0], resolved_client->h_length);
  
	/* Search for the job on the worker node */
	for (server_port = PORT_RANGE_FROM; server_port <= PORT_RANGE_TO; server_port++)
	{
		/* Create the socket everytime (cannot be reused once closed) */
		if ((fd_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1)
		{
			printf("%s: cannot create socket: %s", argv[0], strerror(errno));
			exit(1);
		}

		/* Bind any port number */
		localAddr.sin_family = AF_INET;
		localAddr.sin_addr.s_addr = htonl(INADDR_ANY);
		localAddr.sin_port = htons(0);

		if (bind(fd_socket, (struct sockaddr *)&localAddr, sizeof(localAddr)) == -1)
		{
			printf("%s: cannot bind socket: %s", argv[0], strerror(errno));
			exit(1);
		}

		servAddr.sin_port = htons(server_port);
		if (connect(fd_socket, (struct sockaddr *) &servAddr, sizeof(servAddr)) == -1)
		{
			fprintf(stderr, "Cannot connect to port %d\n", server_port);
			continue;
		}
	
		fprintf(stderr, "Connected to server, waiting for jobId...\n");
		recv(fd_socket, buffer, sizeof(buffer), 0);
		fprintf(stderr, "Received jobId: %s\n", buffer);
	
		/* 
		   Additional part of the requested and received jobids are removed before strcmp:
		   it is assumed that the ramaining part is unique for every batch system.    
		   old version : if (strcmp(buffer, jobId) != 0)
		*/
		if (strcmp(pre_chop(chop(buffer)), pre_chop(chop(jobId))) != 0)
		{
			close(fd_socket);
			fprintf(stderr, "Wrong job on port %d: expected %s, received %s\n", server_port, jobId , buffer);
			continue;
		}
		else
			break;
	}
	if (server_port > PORT_RANGE_TO)
	{
		printf("%s: job %s not found on node %s", argv[0], jobId, workernode);
		exit(1);
	}
	
	fprintf(stderr, "Initiating security context...\n");
	if ((context_handle = initiate_context(credential_handle, "GSI-NO-TARGET", fd_socket)) == GSS_C_NO_CONTEXT)
	{
		printf("%s: cannot initiate security context", argv[0]);
		exit(1);
	}

	if (verify_context(context_handle))
	{
		printf("%s: server certificate mismatch", argv[0]);
		exit(1);
	}

	fprintf(stderr, "Sending proxy file...");
	if ((proxy_file = open(proxy_filename, O_RDONLY)) == -1)
	{
		printf("%s: unable to open proxy file %s", argv[0], proxy_filename);
		exit(1);
	}
	fstat(proxy_file, &proxy_stat); /* Get the proxy size */
	if ((message = (char *)malloc(proxy_stat.st_size + 1)) == NULL)
	{
		printf("%s: unable to allocate read buffer (%d bytes)", argv[0], proxy_stat.st_size + 1);
		exit(1);
	}
	read(proxy_file, message, proxy_stat.st_size);
	close(proxy_file);
	message[proxy_stat.st_size] = 0;
	fprintf(stderr, "File length: %d\n", proxy_stat.st_size);
	send_string(message, context_handle, fd_socket);
	close(fd_socket);

	printf("%s: proxy file successfully sent", argv[0]);	
	exit(0);
}

/* Elimination of server from jobid */
char*
chop(char* long_id)
{
        char* tmpchop=0;
        char* startstring=long_id;
        int chop_count =0;
        while((*(long_id))!= '.')
        {
                if((*(long_id)) == 0)
                return strdup(startstring);
                else
                {
                        chop_count++;
                        long_id++;
                }
        }
        tmpchop=(char*)malloc(chop_count+1);
        memcpy(tmpchop,startstring,chop_count);
        tmpchop[chop_count]=0;
        return
                tmpchop;
}

/* Elimination of gridtype and logfile from jobid */
char*
pre_chop(char* long_id)
{
        char* startstring=long_id;
        int len=strlen(long_id);
        int count=len-1;
        while(long_id[count]!= '/')
        {
                if(count == 0)
                return startstring;
                else count--;
        }
        return &long_id[++count];
}

