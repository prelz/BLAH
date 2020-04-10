#ifndef GLOBUS_UTILS_H
#define GLOBUS_UTILS_H


int grid_proxy_info(const char *proxy_filename);
int grid_proxy_init(const char *src_filename, char *dst_filename,
					int lifetime);
#endif
