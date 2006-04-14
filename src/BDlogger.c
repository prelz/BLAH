/*********************************
*	BDlogger:		 *
* 	Blah for DGAS logger	 *
* suided tool for DGAS logging 	 *
*********************************/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <fcntl.h>

int main (int argc, char** argv)
{

        char *blah_conf=argv[1];
        char *log_line=argv[2];
	int i=0, rc=0, cs=0, result=0, fd = -1, count = 0, slen = 0, slen2 = 0;
        FILE *conf_file=NULL;
        FILE *log_file=NULL;
        char *temp_str=NULL;
        char date_str[1000], jobid_trunc[1000];
        struct flock fl;
        char *AccInfoLogFile=NULL;
        char AccInfoLogFileDated[2000];
        time_t tt;
        struct tm *t_m=NULL;
        char filebuffer[1000];

        if(blah_conf!=NULL) conf_file= fopen(blah_conf,"r");
        else return 1;
        if(conf_file==NULL) return 1;
        slen=strlen("BLAHPD_ACCOUNTING_INFO_LOG=");
       
	/*seek logfile name template in blah.conf */ 
	while(fgets(filebuffer, 1000, conf_file))
        {
                slen2=strlen(filebuffer);
                if(!strncmp(filebuffer,"BLAHPD_ACCOUNTING_INFO_LOG=",slen))
                {
                        count = slen;
                        while((filebuffer[count]!='\n')&& (count < slen2)) count++;
                        AccInfoLogFile=malloc(count-slen+1);
                        memcpy(AccInfoLogFile, &filebuffer[slen], count-slen);
                        filebuffer[count-slen]=0;
                        AccInfoLogFile[count-slen]=0;
                        slen2=slen=0;
                        break;
                }
                memset(filebuffer,0,1000);
        }
        fclose(conf_file);

        /* Add submission time to log file name */
        time(&tt);
        t_m = gmtime(&tt);
        sprintf(date_str,"%04d-%02d-%02d %02d:%02d:%02d", 1900+t_m->tm_year, t_m->tm_mon+1, t_m->tm_mday, t_m->tm_hour, t_m->tm_min, t_m->tm_sec);

        if(AccInfoLogFile!=NULL)
        {
                sprintf(AccInfoLogFileDated,"%s-%04d%02d%02d", AccInfoLogFile, 1900+t_m->tm_year, t_m->tm_mon+1, t_m->tm_mday);
                if (AccInfoLogFileDated==NULL)
                {
                        free(AccInfoLogFile);
                        return 1;
                }

                log_file=fopen(AccInfoLogFileDated,"a");
                if (log_file==NULL)
                {
                        free(AccInfoLogFile);
                        return 1;
                }
        }
        /* get the lock */
        fd = fileno(log_file);
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_CUR;
        do{
                result = fcntl( fd, F_SETLK, &fl);
        } while((result == -1)&&(errno == EINTR));
	
	/* writes log line */
        cs = fwrite(log_line ,1, strlen(log_line), log_file);
        cs = fwrite("\n" ,1, 1, log_file); 
	fl.l_type = F_UNLCK;
        /* release the lock */
        result = fcntl( fd, F_SETLK, &fl);
        fclose(log_file);
        return 0;
}









