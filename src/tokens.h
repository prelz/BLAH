/***************************************************************************
 *  filename  : tokens.h
 *  authors   : Salvatore Monforte <salvatore.monforte@ct.infn.it>
 *  copyright : (C) 2001 by INFN
 ***************************************************************************/

// $Id: tokens.h,v 1.2 2005/03/23 10:32:24 drebatto Exp $

/**
 * @file tokens.h
 * @brief The definition for token transmission and reception.
 * This file provides a couple of methods to send and receive tokens.
 * @author Salvatore Monforte salvatore.monforte@ct.infn.it
 * @author comments by Marco Pappalardo marco.pappalardo@ct.infn.it and Salvatore Monforte
 */

#ifndef _TOKENS_H
#define _TOKENS_H

/*
#include <unistd.h>
*/

#ifdef __cplusplus
extern "C++" {
#endif

int send_token(void *arg, void * token, size_t  token_length);
int get_token(void *arg, void ** token, size_t * token_length);

#ifdef __cplusplus
}
#endif
#endif /* _TOKENS_H */

/*
  Local Variables:
  mode: c++
  End:
*/

