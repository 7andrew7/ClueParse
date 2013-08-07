package edu.washington.escience.commoncrawl;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;

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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.washington.escience.util.UrlNormalizer;

public final class GraphExtractor extends Configured implements Tool {

	public static final class VertexRecord {
		public String normalizedURLStr = null;
		public String hash = null;
		public boolean success = false;
		public String cc_content_type = null;
		public String http_result = null;
		public String http_content_length = null;
		public String http_mime_type = null;
		public String failure_reason = null;
		public String failure_detail = null;
	
		@Override
		public String toString() {
			return String.format("%s\t%s\t%b\t%s\t%s\t%s\t%s\t%s\t%s", normalizedURLStr, hash, success, cc_content_type, http_result,
					http_content_length, http_mime_type, failure_reason, failure_detail);
		}		
	}
	
	// extract a string member from a JSON object, or return null if no such member exists
	private static String getStringJsonMember(JsonObject obj, String memberName) {
		JsonElement elem = obj.get(memberName);
		if (elem == null)
			return null;
		return elem.getAsString();
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
	    String inputPath = "file:///scratch/cc_data/raw/metadata-01849";
	    String outputPath = "file:///scratch/cc_data/parsed";
	    
	    if (args.length >= 1)
	    	inputPath = args[0];
	    if (args.length >= 2)
	    	outputPath = args[1];

	    // Scan the provided input path for ARC files.
	    System.out.println("scanning input path: "+ inputPath);
	    FileSystem fs_in = FileSystem.get(new URI(inputPath), conf);

	    FileSystem fs_out = FileSystem.get(new URI(outputPath), conf);
	    OutputStream vertex_os = fs_out.create(new Path(outputPath + "/vertexes.out"));
	    PrintWriter vertex_out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(vertex_os)));
	    
	    OutputStream edge_os = fs_out.create(new Path(outputPath + "/edges.out"));
	    PrintWriter edge_out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(edge_os)));
	    
	    Path rootPath = new Path(inputPath);
	    FileStatus[] status = fs_in.globStatus(rootPath);

	    System.out.println("Found " + status.length + " metadata files");

	    int count = 0;
    	int goodPages = 0;
    	int failedPages = 0;
    	int numLinks = 0;

	    for (FileStatus stat: status) {
	    	count++;
	    	Path path = stat.getPath();
	    	System.out.printf("[%d/%d %f%%] %s\n", count, status.length, 100 * ((float)count/ status.length), path); 
	    			
	    	Reader reader = new SequenceFile.Reader(fs_in, path, conf);
	    	Text sourceUrlText = new Text();
	    	Text jsonText = new Text();

	    	while (reader.next(sourceUrlText, jsonText)) { // foreach web page...
	    		VertexRecord record = new VertexRecord();

	    		// normalize the URL, extract its hash
	    		record.normalizedURLStr = UrlNormalizer.normalizeURLString(sourceUrlText.toString(), null);
	    		record.hash = UrlNormalizer.URLStringToIDString(record.normalizedURLStr);
	    		URL source_url = new URL(sourceUrlText.toString());

	    		// Extract the content JSON blob
	    		JsonParser jsonParser = new JsonParser();
	    		JsonObject jsonObj = jsonParser.parse(jsonText.toString()).getAsJsonObject();
	    		
	    		String disp = getStringJsonMember(jsonObj, "disposition");
	    		if (disp == null || disp.equals("FAILURE")) {
	    			failedPages++;
	    			record.success = false;
	    			record.failure_reason = getStringJsonMember(jsonObj, "failure_reason");
	    			record.failure_detail = getStringJsonMember(jsonObj, "failure_detail");
	    		} else {
	    			goodPages++;
	    			record.success = true;
	    			record.http_result = getStringJsonMember(jsonObj, "http_result");
	    			record.http_mime_type = getStringJsonMember(jsonObj, "mime_type");
	    			record.http_content_length = getStringJsonMember(jsonObj, "content_len");
	    			
	    			JsonObject content = jsonObj.getAsJsonObject("content");
	    			if (content != null) {
		    			record.cc_content_type = getStringJsonMember(content, "type");
	    		
		    			// Iterate over the array of links in the content object
		    			JsonArray links = content.getAsJsonArray("links");
		    			if (links != null) {	    		
		    				for (JsonElement elem : links) {
		    					JsonObject link = elem.getAsJsonObject();
		    					String linkType = link.get("type").getAsString();
		    					if (linkType == null)
		    						continue;
		    					if (!linkType.equals("a"))
		    						continue;
		    					
		    					String linkUrlStr = link.get("href").getAsString();
		    					String normalizedLinkUrlStr = UrlNormalizer.normalizeURLString(linkUrlStr, source_url);
		    					String dest_hash = UrlNormalizer.URLStringToIDString(normalizedLinkUrlStr);
	    			
		    					edge_out.println(record.hash + "\t" + dest_hash);
		    					numLinks++;
		    				} // foreach link
		    			}
	    			}
	    		}
    			vertex_out.println(record.toString());
	    	} // foreach webpage
	    	
	    	vertex_out.close();
	    	vertex_os.close();
	    	edge_out.close();
	    	edge_os.close();
	    	reader.close();
	   }
	    
	    System.out.println("SUCCESS: " + goodPages + " FAILURES: " + failedPages + " links: " + numLinks);
	    return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new GraphExtractor(), args);
		System.exit(exitCode);
	}
}