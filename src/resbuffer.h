#define BUFFER_SAVE       1
#define BUFFER_DONT_SAVE  0
#define BUFFER_FLUSH      1
#define BUFFER_DONT_FLUSH 0 

/* Exported functions */

int init_resbuffer(void);
char* get_lines(const int flush_bf);
int num_results(void);
int push_result(const char* s, const int save);

