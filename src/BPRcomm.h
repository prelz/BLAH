#ifndef _BPRCOMM_H
#define _BPRCOMM_H

#define PORT_RANGE_FROM   20001
#define PORT_RANGE_TO     20100

#define BPR_DELEGATE_PROXY_OK 0
#define BPR_DELEGATE_PROXY_ERROR -1
#define BPR_RECEIVE_DELEGATED_PROXY_OK 0
#define BPR_RECEIVE_DELEGATED_PROXY_ERROR -1

/* Transmission functions */
int send_string(const char *s, gss_ctx_id_t gss_context, int sck);
int receive_string(char **s, gss_ctx_id_t gss_context, int sck);
int delegate_proxy(const char *proxy_file, gss_cred_id_t cred_handle, gss_ctx_id_t gss_context, int sck);
int receive_delegated_proxy(char **s, gss_ctx_id_t gss_context, int sck);

/* GSS functions */
gss_cred_id_t acquire_cred(const gss_cred_usage_t cred_usage);
gss_ctx_id_t accept_context(gss_cred_id_t credential_handle, char **client_name, int sck);
gss_ctx_id_t initiate_context(gss_cred_id_t credential_handle, const char *server_name, int sck);
int verify_context(gss_ctx_id_t context_handle);
OM_uint32 get_cred_lifetime(const gss_cred_id_t credential_handle);

#endif /* ifndef _BPRCOMM_H */
