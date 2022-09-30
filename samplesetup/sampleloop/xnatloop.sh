#!/bin/bash

#Simple bash loop. first argument is a csv file, second is output directory
#since flags are not positional, you can add them to the lines in your list (see example)
#example call:
# xnatloop.sh sample.csv /path/to/download 
SUBJECTS=${1}
OUTPUT=${2}
for i in $( cat ${1} ) ; do
	xnat-tools -i $${i//\,/\ } -o ${2}
done
	

