/*
#  File:     globus_utils.c
#
#  Author:   Jaime Frey/ (HT)Condor
#
#  Revision history:
#   19 Apr 2013 - Original release.
#
#  Description:
#   Direct callouts for Globus proxy handling.
#
#
#  Copyright (c) Members of the EGEE Collaboration. 2007-2010. 
#
#    See http://www.eu-egee.org/partners/ for details on the copyright
#    holders.  
#  
#    Licensed under the Apache License, Version 2.0 (the "License"); 
#    you may not use this file except in compliance with the License. 
#    You may obtain a copy of the License at 
#  
#        http://www.apache.org/licenses/LICENSE-2.0 
#  
#    Unless required by applicable law or agreed to in writing, software 
#    distributed under the License is distributed on an "AS IS" BASIS, 
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#    See the License for the specific language governing permissions and 
#    limitations under the License.
#
*/

#include "globus_gsi_credential.h"
#include "globus_gsi_proxy.h"

const char *grid_proxy_errmsg = NULL;

int activate_globus()
{
	static int active = 0;

	if (active) {
		return 0;
	}

	if ( globus_module_activate(GLOBUS_GSI_CREDENTIAL_MODULE) ) {
		grid_proxy_errmsg = "failed to activate Globus";
		return -1;
	}

	if ( globus_module_activate(GLOBUS_GSI_PROXY_MODULE) ) {
		grid_proxy_errmsg = "failed to activate Globus";
		return -1;
	}

	active = 1;
	return 0;
}

/* Returns lifetime left on proxy, in seconds.
 * 0 means proxy is expired.
 * -1 means an error occurred.
 */
int grid_proxy_info(const char *proxy_filename)
{
	globus_gsi_cred_handle_t handle = NULL;
	time_t time_left = -1;

	if ( activate_globus() < 0 ) {
		return -1;
	}

	if (globus_gsi_cred_handle_init(&handle, NULL)) {
		grid_proxy_errmsg = "failed to initialize Globus data structures";
		goto cleanup;
	}

	// We should have a proxy file, now, try to read it
	if (globus_gsi_cred_read_proxy(handle, proxy_filename)) {
		grid_proxy_errmsg = "unable to read proxy file";
		goto cleanup;
	}

	if (globus_gsi_cred_get_lifetime(handle, &time_left)) {
		grid_proxy_errmsg = "unable to extract expiration time";
		goto cleanup;
	}

	if ( time_left < 0 ) {
		time_left = 0;
	}

 cleanup:
	if (handle) {
		globus_gsi_cred_handle_destroy(handle);
	}

	return time_left;
}

/* Writes new proxy derived from existing one. Argument lifetime is the
 * number of seconds until expiration for the new proxy. A 0 lifetime
 * means the same expiration time as the source proxy.
 * Returns 0 on success and -1 on error.
 */
int grid_proxy_init(const char *src_filename, char *dst_filename,
					int lifetime)
{
	globus_gsi_cred_handle_t src_handle = NULL;
	globus_gsi_cred_handle_t dst_handle = NULL;
	globus_gsi_proxy_handle_t dst_proxy_handle = NULL;
	int rc = -1;
	time_t src_time_left = -1;
	globus_gsi_cert_utils_cert_type_t cert_type = GLOBUS_GSI_CERT_UTILS_TYPE_LIMITED_PROXY;

	if ( activate_globus() < 0 ) {
		return -1;
	}

	if (globus_gsi_cred_handle_init(&src_handle, NULL)) {
		grid_proxy_errmsg = "failed to initialize Globus data structures";
		goto cleanup;
	}

	// We should have a proxy file, now, try to read it
	if (globus_gsi_cred_read_proxy(src_handle, src_filename)) {
		grid_proxy_errmsg = "unable to read proxy file";
		goto cleanup;
	}

	if (globus_gsi_cred_get_lifetime(src_handle, &src_time_left)) {
		grid_proxy_errmsg = "unable to extract expiration time";
		goto cleanup;
	}
	if ( src_time_left < 0 ) {
		src_time_left = 0;
	}

	if (globus_gsi_proxy_handle_init( &dst_proxy_handle, NULL )) {
		grid_proxy_errmsg = "failed to initialize Globus data structures";
		goto cleanup;
	}

		// lifetime == desired dst lifetime
		// src_time_left == time left on src
	if ( lifetime == 0 || lifetime > src_time_left ) {
		lifetime = src_time_left;
	}
	if (globus_gsi_proxy_handle_set_time_valid( dst_proxy_handle, lifetime/60 )) {
		grid_proxy_errmsg = "unable to set proxy expiration time";
		goto cleanup;
	}

	if (globus_gsi_proxy_handle_set_type( dst_proxy_handle, cert_type)) {
		grid_proxy_errmsg = "unable to set proxy type";
		goto cleanup;
	}

	if (globus_gsi_proxy_create_signed( dst_proxy_handle, src_handle, &dst_handle)) {
		grid_proxy_errmsg = "unable to generate proxy";
		goto cleanup;
	}

	if (globus_gsi_cred_write_proxy( dst_handle, dst_filename )) {
		grid_proxy_errmsg = "unable to write proxy file";
		goto cleanup;
	}

	rc = 0;

 cleanup:
	if (src_handle) {
		globus_gsi_cred_handle_destroy(src_handle);
	}
	if (dst_handle) {
		globus_gsi_cred_handle_destroy(dst_handle);
	}
	if ( dst_handle ) {
		globus_gsi_proxy_handle_destroy( dst_proxy_handle );
	}

	return rc;
}

#ifdef GLOBUS_UTILS_TEST_CODE

#include <stdio.h>
#include <stdlib.h>

int
main(int argc, char *argv[])
{
	char *proxy_in;
	char *proxy_out;
	const char *lmt_proxy_ext=".LMT";
	int timeleft, rc = -1;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <test proxy name>\n", argv[0]);
		return 1;
	}
	proxy_in = argv[1];

	timeleft = grid_proxy_info(proxy_in);
	printf("Lifetime of proxy %s: %d\n", proxy_in, timeleft);
	proxy_out = (char *) malloc(strlen(proxy_in) + strlen(lmt_proxy_ext) +1);
	if (proxy_out != NULL) {
		strcpy(proxy_out, proxy_in);
		strcat(proxy_out, lmt_proxy_ext);
		printf("Now trying to create limited proxy %s.\n", proxy_out);
		rc = grid_proxy_init(proxy_in, proxy_out, timeleft);
		printf("grid_proxy_init returns %d.\n", rc);
		free(proxy_out);
	}
	
	return rc;
}
#endif /* defined GLOBUS_UTILS_TEST_CODE */
