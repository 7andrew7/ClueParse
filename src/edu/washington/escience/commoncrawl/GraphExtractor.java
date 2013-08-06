package edu.washington.escience.commoncrawl;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.washington.escience.util.UrlNormalizer;
import edu.washington.escience.warc.WarcVertices;

public final class GraphExtractor extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
	    String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/1341690166822/metadata-*";
	    if (args.length >= 1)
	    	inputPath = args[0];

	    // Scan the provided input path for ARC files.
	    System.out.println("scanning input path: "+ inputPath);
	    FileSystem fs = FileSystem.get(new URI("s3n://aws-publicdatasets/common-crawl"), conf);

	    Path rootPath = new Path(inputPath);
	    FileStatus[] status = fs.globStatus(rootPath);

	    System.out.println("Found " + status.length + " metadata files");

	    for (FileStatus stat: status) {
	    	Path path = stat.getPath();
	    	System.out.println(path);

	    	Reader reader = new SequenceFile.Reader(fs, path, conf);
	    	Text key = new Text();
	    	Text value = new Text();

	    	
	    	while (reader.next(key, value)) { // foreach web page...
	    		/*
	    		JsonParser jsonParser = new JsonParser();
	    		JsonObject jsonObj    = jsonParser.parse(value.toString()).getAsJsonObject();
	    		JsonObject content = jsonObj.getAsJsonObject("content");
	    		if (content != null) {
	    			String type = content.get("type").getAsString();
	    			System.out.println(type);
	    		}
	    		*/
	    		
	    		// Emit an entry for the vertex
	    		String normalizedUrlStr = UrlNormalizer.normalizeURLString(key.toString(), null);
	    		String id = UrlNormalizer.URLStringToIDString(normalizedUrlStr);
	    		System.out.println(id + "\t" + normalizedUrlStr);
	    	}
	    	
	    	reader.close();
	   }
	    
	    return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WarcVertices(), args);
		System.exit(exitCode);
	}
}
