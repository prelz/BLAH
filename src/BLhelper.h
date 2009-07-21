#include <unistd.h>             /*  for ssize_t data type  */
#include <pthread.h>

#define LISTENQ        (1024)   /*  Backlog for listen()   */


/*  Function declarations  */

ssize_t Readline(int fd, void *vptr, size_t maxlen);
ssize_t Writeline(int fc, const void *vptr, size_t maxlen);

pthread_mutex_t writeline_mutex = PTHREAD_MUTEX_INITIALIZER;

