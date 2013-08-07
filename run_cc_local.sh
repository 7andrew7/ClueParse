#!/bin/bash

LIBJARS=`./emit_libjars.py`
HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

hadoop jar ./build/libs/ClueParse-0.1.jar edu.washington.escience.commoncrawl.GraphExtractor -libjars ${LIBJARS}
