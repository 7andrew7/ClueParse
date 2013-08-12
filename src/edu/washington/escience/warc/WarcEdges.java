package edu.washington.escience.warc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
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
import org.jwat.warc.WarcRecord;

import edu.washington.escience.util.UrlNormalizer;
import edu.washington.escience.util.WholeFileInputFormat;

/**
 * Produce an edge table of the form:
 * 
 * uri_id1 uri_id2
 */
public class WarcEdges extends Configured implements Tool {

	static enum CounterTypes {BAD_SOURCE_URL, BAD_DEST_URL , PAYLOAD_PARSE_ERROR};
	
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
	
	public static class Map extends WarcMapper {
		
		@Override
		public void processRecord(WarcRecord record, Context context) {
			Text sourceText = new Text();
			Text destText = new Text();
			
			// Extract the URL of the source page
			String targetUrlStr = record.header.warcTargetUriStr;
			if (targetUrlStr == null)
				return;
	
			URL contextURL = null;
			String normalizedSourceUrlStr = null;
			String sourceHash = null;
				
			try {
				// Create a URL object for the source page; this is used to normalize relative references
				contextURL = new URL(targetUrlStr);
					
				// Calculate the id of the source page
				normalizedSourceUrlStr = UrlNormalizer.normalizeURLString(targetUrlStr, null);
				sourceHash = UrlNormalizer.URLStringToIDString(normalizedSourceUrlStr);
			} catch(Exception ioe) {
				ioe.printStackTrace();
				context.getCounter(CounterTypes.BAD_SOURCE_URL).increment(1);
				return;
			}
									       
			// Extract the set of links by parsing the HTML
			Payload payload = record.getPayload();
			if (payload == null)
				return;
							
			InputStream is = payload.getInputStream();
			List<String> links = null;
				
			try {
				links = getLinks(is);
			} catch(Exception ex) {
				ex.printStackTrace();
				context.getCounter(CounterTypes.PAYLOAD_PARSE_ERROR).increment(1);
				return;
			}
			
			for (String link : links) {
				try {
					// Calculate the id of the destination page
					String normalizedDestURL = UrlNormalizer.normalizeURLString(link, contextURL);
					String destHash = UrlNormalizer.URLStringToIDString(normalizedDestURL);
					
					// Emit a (source, dest) tuple
					sourceText.set(sourceHash);
					destText.set(destHash);		        		
					context.write(sourceText, destText);
				} catch (Exception ex) {
					context.getCounter(CounterTypes.BAD_DEST_URL).increment(1);
					continue;
				}
			} // foreach link
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text source, Iterable<Text> dests, Context context)
				throws IOException, InterruptedException {
			for (Text dest : dests) {
				context.write(source, dest);
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String inputPath = "/scratch/warc_data/1000wb-00.warc.gz";
		String outputPath = "/scratch/warc_data/processed/edges";
		
		if (args.length >= 1)
			inputPath = args[0];
		if (args.length >= 2)
			outputPath = args[1];
		
		System.out.println("input: " + inputPath);
		System.out.println("output: " + outputPath);
	    
	    Job job = new Job(getConf());
	    job.setJarByClass(WarcEdges.class);
	    
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(0);

		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 :  1;
	}
	    	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WarcEdges(), args);
		System.exit(exitCode);
	}
}
