#!/bin/bash

dir="/usr/local/spark/resources/fileshare/EDIP-Code"

if [ -d "$dir" -a ! -h "$dir" ]
then
    cd "$dir"
	git reset --hard HEAD
    git pull
else
    cd "/usr/local/spark/resources/fileshare"
    git clone https://pwascazapp01.ibddomain.net/AdvisorGroup/Data%20and%20Analytics/_git/EDIP-Code
fi

