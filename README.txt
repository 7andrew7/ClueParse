
Map/Reduce functions for parsing the ClueWeb12 data set.  There are two
programs:

edu.washington.escience.warc.WarcVertices: Emit a series of files with the
form: id url.  The ID is simply a subset of the SHA-1 hash of the URL.

edu.washington.escience.warc.WarcEdges: Emit a series of files with the form:
source_id dest_id.

* Requirements:

You need Java1.6 and gradle.

* Building:

gradle build

* Running:

First, you need to setup some classpaths:

export LIBJARS=`./emit_libjars.py`
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

hadoop jar ClueParse-0.1.jar edu.washington.escience.warc.WarcVertices -libjars ${LIBJARS} /datasets/clue/Disk1/ClueWeb12_00/0000wb/0000wb-00.warc.gz /output_dir/
