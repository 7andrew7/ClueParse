#!/bin/bash

LIBJARS=`./emit_libjars.py`
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

hadoop jar ./build/libs/ClueParse-0.1.jar edu.washington.escience.warc.WarcVertices -libjars ${LIBJARS} "/datasets/clue/Disk[1234]/ClueWeb12_??/*/*.warc.gz"  /processed/ClueWeb12/vertexes
