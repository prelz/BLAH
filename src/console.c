/*
#  File:     console.c
#
#  Author:   David Rebatto
#  e-mail:   David.Rebatto@mi.infn.it
#
#
#  Revision history:
#   20 Mar 2004 - Original release
#
#  Description:
#   Process console commands.
#
#
#  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
#  All rights reserved.
#  See http://grid.infn.it/grid/license.html for license details.
#

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
processConsoleCommand()
{
    char buffer[256];
                                                                                                                                               
    fgets(buffer, sizeof(buffer), stdin);
    if (buffer[strlen(buffer) - 1] == '\n') buffer[strlen(buffer) - 1] = '\0';
    
    if (buffer[0] == '\0')
        return(0);
    else if (strcasecmp("QUIT", buffer) == 0)
        return(1);
    else
        fprintf(stderr, "Unknown command %s\n", buffer);
                                                                                                                                               
    return(0);
}

