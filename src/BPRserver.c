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
#include <signal.h>
#include <sys/wait.h>
#include <globus_gss_assist.h>
#include <globus_gsi_proxy.h>
#include <globus_gsi_system_config.h>
#include "tokens.h"
#include "BPRcomm.h"

#define DEFAULT_POLL_INTERVAL 60
#define DEFAULT_MIN_PROXY_LIFETIME 180

int
main(int argc, char **argv)
{
	int fd_socket, read_socket;
	int listen_port;
	struct sockaddr_in recvs;
	struct timeval tv;
	struct protoent *prot_descr;
	struct sockaddr_in cli_addr;
	char *client_name = NULL;
	struct hostent *resolved_client;
	int addr_size = sizeof(cli_addr);
	fd_set readfs, masterfs;
	int retcod;
	int exit_program = 0;
	pid_t monitored_pid;
	char *jobId;
	char *message;
	int proxy_file;
	int poll_interval = 0;
	int min_proxy_lifetime = 0;
						    
	gss_cred_id_t	credential_handle = GSS_C_NO_CREDENTIAL;
	gss_ctx_id_t	context_handle = GSS_C_NO_CONTEXT;
	OM_uint32       major_status = 0, minor_status = 0;

	char *proxy_file_name[MAXPATHLEN];
	char *proxy_tmp_name = NULL;
	       
	if (globus_gsi_sysconfig_get_proxy_filename_unix(proxy_file_name, GLOBUS_PROXY_FILE_INPUT) != 0)
	{
		fprintf(stderr, "Cannot find proxy filename\n");
		exit(1);
	}
	fprintf(stderr, "Proxy file: %s\n", *proxy_file_name);
	
	/* Get the arguments
	 * ----------------- */
	if (argc < 2)
	{
		fprintf(stderr, "At least process pid is required\n");
		exit(1);
	}
	monitored_pid = atoi(argv[1]);
	if (argc > 2) poll_interval = atoi(argv[2]);
	if (!poll_interval) poll_interval = DEFAULT_POLL_INTERVAL; /* 0 IS NOT ACCEPTABLE */
	if (argc > 3) min_proxy_lifetime = atoi(argv[3]);
	if (!min_proxy_lifetime) min_proxy_lifetime = DEFAULT_MIN_PROXY_LIFETIME;
	if (argc > 4) jobId = argv[4];
	else          exit(3);

	/* Open the socket 
	 * --------------- */
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

	/* Cicle through the port range */
	for (listen_port = PORT_RANGE_FROM; listen_port <= PORT_RANGE_TO; listen_port++)
	{
		recvs.sin_family = AF_INET;
		recvs.sin_addr.s_addr = htonl(INADDR_ANY);
		recvs.sin_port = htons(listen_port);
		if (bind(fd_socket,(struct sockaddr *)&recvs,sizeof(recvs)) == 0) 
			break;
	}

	if (listen_port > PORT_RANGE_TO)
	{
		fprintf(stderr, "Cannot bind to any port in the given range (%d-%d)\n", PORT_RANGE_FROM, PORT_RANGE_TO);
		exit(1);
	}
	else fprintf(stderr, "Bind on port %d\n", listen_port);

	if (listen(fd_socket,1) == -1)
	{
		fprintf(stderr, "Cannot listen from socket: %s\n", strerror(errno));
		exit(1);
	}


	/* Main loop
	 * --------- */
	while (!exit_program)
	{
		FD_ZERO(&masterfs);
		FD_SET(fd_socket, &masterfs);

		tv.tv_sec = poll_interval;
		tv.tv_usec = 0;
	
		readfs = masterfs;
		if ((retcod = select(FD_SETSIZE, &readfs, (fd_set *)NULL, (fd_set *)NULL, &tv)) == -1)
		{
			perror("Select error");
			close(fd_socket);
			exit(1);
		}
		if (retcod)
		{
		        if (FD_ISSET(fd_socket, &readfs))
		        {
				if ((read_socket = accept(fd_socket, (struct sockaddr *)&cli_addr, &addr_size)) == -1)
				{
					fprintf(stderr,"\nCannot accept connection: %s\n", strerror(errno));
					exit(1);
				}

				/* Don't forget to terminate the string... */
				send(read_socket, jobId, strlen(jobId)+1, 0);
				
				FD_ZERO(&masterfs);
				FD_SET(read_socket, &masterfs);

				fprintf(stderr, "Entering select...\n");
				tv.tv_sec = 15;
				tv.tv_usec = 0;
				readfs = masterfs;
				if (select(FD_SETSIZE, &readfs, (fd_set *)NULL, (fd_set *)NULL, &tv) <= 0)
				{
					perror("Select error");
					exit(1);
				}

				fprintf(stderr, "Acquiring credentials...\n");
				if ((credential_handle = acquire_cred(GSS_C_ACCEPT)) == GSS_C_NO_CREDENTIAL)
				{
					fprintf(stderr, "Unable to acquire credentials, exiting...\n");
					exit(1);
				}
	
				fprintf(stderr, "Accepting security context...\n");
				if ((context_handle = accept_context(credential_handle, &client_name, read_socket)) == GSS_C_NO_CONTEXT)
				{
					fprintf(stderr, "Cannot accept security context...\n");
					fprintf(stderr, "Listening for further connections...\n");
					gss_release_cred(&major_status, &credential_handle);
					close(read_socket);
				}
				else
				{
					fprintf(stderr, "Security context accepted\n");
					fprintf(stderr, "Client name: %s\n", client_name);
					fprintf(stderr, "Receiving new proxy...\n");
					receive_string(&message, context_handle, read_socket);
					proxy_tmp_name = strdup("/tmp/tmp_proxy_XXXXXX");
					proxy_file = mkstemp(proxy_tmp_name);
					if (proxy_file == -1)
					{
						perror("Cannot create temp file");
						exit(1);
					}
					write(proxy_file, message, strlen(message));
					close(proxy_file);
					fprintf(stderr, "Renaming %s to %s\n", proxy_tmp_name, *proxy_file_name);
					rename(proxy_tmp_name, *proxy_file_name);
					gss_release_cred(&major_status, &credential_handle);
					gss_delete_sec_context(&minor_status, &context_handle, GSS_C_NO_BUFFER);
					if (message) free (message);
					free(proxy_tmp_name);
					close(read_socket);
					fprintf(stderr, "New proxy saved.\n");
					fprintf(stderr, "Listening for further connections...\n");
				}
			}
		}
		else /* listen timeout expired */
		{
			fprintf(stderr, "Time to see if process %d is alive... ", monitored_pid);
			if ( kill(monitored_pid, 0) )
			{
				fprintf(stderr, "No, it is terminated.\n");
				exit_program = 1;
			}
			else
			{
				fprintf(stderr, "Yes, it is alive.\n");
				fprintf(stderr, "Verifying credential lifetime...\n");
				if ((credential_handle = acquire_cred(GSS_C_ACCEPT)) == GSS_C_NO_CREDENTIAL)
				{
					fprintf(stderr, "Unable to acquire credentials, exiting...\n");
					exit(1);
				}
				fprintf(stderr, "%d seconds left.\n", get_cred_lifetime(credential_handle));
				if (get_cred_lifetime(credential_handle) < min_proxy_lifetime)
				{
					fprintf(stderr, "Killing the job with SIGTERM...\n");
					if (kill(monitored_pid, SIGTERM))
					{
						perror("Unable to kill the job:");
						fprintf(stderr, "Sleeping 30 seconds and retrying with SIGKILL...\n");
						sleep(30);
						kill(monitored_pid, SIGKILL);
						fprintf(stderr, "SIGKILL sent, exiting...\n");
					}
					exit_program = 1;
				}
				gss_release_cred(&major_status, &credential_handle);
			}
		}
	} /* while main loop */
}

