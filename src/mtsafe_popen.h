/* Function prototypes */

FILE * mtsafe_popen(const char *command, const char *type);
int mtsafe_pclose(FILE *stream);

/* Thread safe command execution and output capture */
int exe_getout(char *const command, char *const environment[], char **cmd_output);
int exe_getouterr(char *const command, char *const environment[], char **cmd_output, char **cmd_error);
/* command:     the command to execute
   environment: extra environment to be set before executing <command>
                in addition to the current environment
   cmd_output:  dinamically allocated buffer contaning the standard output
   cmd_error:   dinamically allocated buffer contaning the standard error
   With exe_getout the standard error is printed on stderr, preceded by <command> itself
*/

