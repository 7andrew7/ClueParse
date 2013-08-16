
Map/Reduce functions for parsing the ClueWeb12 data set.  There are two
programs:

edu.washington.escience.warc.WarcVertices: Emit a series of files with the
form: id url.  The ID is simply a subset of the SHA-1 hash of the URL.

edu.washington.escience.warc.WarcEdges: Emit a series of files with the form:
source_id dest_id.

* Requirements:

You need Java 1.6 and gradle.

* Building:

gradle build

* Running:

./run_warc_vertexes_all.sh
./run_warc_edges_all.sh
