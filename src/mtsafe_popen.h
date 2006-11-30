/* Function prototypes */

FILE * mtsafe_popen(const char *command, const char *type);
int mtsafe_pclose(FILE *stream);
int exe_getout(char *const command, char *const environment[], char **cmd_output);
