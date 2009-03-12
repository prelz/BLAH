#!/bin/bash

[ -f ${GLITE_LOCATION:-/opt/glite}/etc/blah.config ] && . ${GLITE_LOCATION:-/opt/glite}/etc/blah.config

if [ ! -z "$sge_root" -a -f "$sge_root/${sge_cell:-default}/settings.sh" ]
then
  . $sge_root/${sge_cell:-default}/settings.sh
fi

jnr=0
jc=0
for job in  $@ ; do
        jnr=$(($jnr+1))
done
for  job in  $@ ; do
        requested=`echo $job | sed 's/^.*\///'`
	requestedshort=`expr match "$requested" '\([0-9]*\)'`

        qdel $requestedshort >/dev/null 2>&1
        if [ "$?" == "0" ] ; then
                if [ "$jnr" == "1" ]; then
                        echo " 0 No\\ error"
                else
                        echo .$jc" 0 No\\ error"
                fi
        else
                if [ "$jnr" == "1" ]; then
                        echo " 1 Error"
                else
                        echo .$jc" 1 Error"
                fi
        fi
        jc=$(($jc+1))
done

