#!/bin/bash
if [ $# == 0 ] ; then
	find ../../output -name "*.xml" -exec rm {} \;
	find ../../output -name "*.sql" -exec rm {} \;
	find ../../output -name "*.csv" -exec rm {} \;
elif [ "$1" == "-html" ] ; then
    find ../../output -name "*.xml" -exec rm {} \;
    find ../../output -name "*.sql" -exec rm {} \;
    find ../../output -name "*.csv" -exec rm {} \;
	find ../../output -name "*.html" -exec rm {} \;

fi
