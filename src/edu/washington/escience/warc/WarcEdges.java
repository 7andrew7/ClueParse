package edu.washington.escience.warc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.common.Payload;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import edu.washington.escience.util.UrlNormalizer;
import edu.washington.escience.util.WholeFileInputFormat;

/**
 * Produce an edge table of the form:
 * 
 * uri_id1 uri_id2
 */
public class WarcEdges extends Configured implements Tool {

	private static List<String> getLinks(InputStream is) throws IOException {
		List<String> links = new ArrayList<String>();
		String str = IOUtils.toString(is, "UTF-8");
		
		Document doc = Jsoup.parse(str);
		Elements elems = doc.select("a[href]"); // a with href
		for (Element elem : elems) {
			links.add(elem.attr("href"));
		}	
		
		return links;		
	}
	
	public static class Map extends Mapper<NullWritable, BytesWritable, Text, Text > {
		
		@Override
		public void map(NullWritable key, BytesWritable value, Context context) 
				throws IOException, InterruptedException {
			ByteArrayInputStream bis = new ByteArrayInputStream(value.getBytes());
			Text sourceText = new Text();
			Text destText = new Text();
			
			// Work around broken gzip decoder in jwat
			InputStream in = new GZIPInputStream(bis);
			
			WarcReader reader = WarcReaderFactory.getReaderUncompressed(in);
			WarcRecord record;
			while ( (record = reader.getNextRecord()) != null ) {
				// Extract the URL of the source page
				String targetUrlStr = record.header.warcTargetUriStr;
				if (targetUrlStr == null)
					continue;

				try {
					// Create a URL object for the source page; this is used to normalize relative references
					URL contextURL = new URL(targetUrlStr);
					
					// Calculate the id of the source page
		        	String normalizedSourceUrl = UrlNormalizer.normalizeURLString(targetUrlStr, null);
		        	String sourceHash = UrlNormalizer.URLStringToIDString(normalizedSourceUrl);
		        	
		        	// Extract the set of links by parsing the HTML
			       	Payload payload = record.getPayload();
		        	if (payload != null) {
		        		InputStream is = payload.getInputStream();
		        		List<String> links = getLinks(is);
		        		for (String link : links) {
		        			// Calculate the id of the destination page
		        			String normalizedDestURL = UrlNormalizer.normalizeURLString(link, contextURL);
		        			String destHash = UrlNormalizer.URLStringToIDString(normalizedDestURL);
		        			
		        			// Emit a (source, dest) tuple
		        			sourceText.set(sourceHash);
		        			destText.set(destHash);		        		
		    				context.write(sourceText, destText);
		        		}
		        	}
				} catch(IOException ioe) {
					// increment error count?
				}							
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
	    if (args.length != 2) {
	        System.err.printf("Usage: %s [generic options] <input> <output>\n",
	            getClass().getSimpleName());
	        ToolRunner.printGenericCommandUsage(System.err);
	        return -1;
	      }
	    
	    Job job = new Job(getConf());
	    job.setJarByClass(WarcEdges.class);
	    
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Nothing to reduce; this ensures that our mapper's output goes to HDFS
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 :  1;
	}
	    	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WarcEdges(), args);
		System.exit(exitCode);
	}
}
