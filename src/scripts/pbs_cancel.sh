#!/bin/bash

requested=`echo $1 | sed 's/^.*\///'`
qdel $requested
