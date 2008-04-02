#define ALLOC_CHUNKS 32768
#define ASYNC_MODE_OFF 0
#define ASYNC_MODE_ON  1

/* Exported functions */

int init_resbuffer(void);
int push_result(const char* s);
char* get_lines(void);

