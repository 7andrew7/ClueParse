package edu.washington.escience.warc;

import java.io.IOException;

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

import org.jwat.warc.WarcRecord;

import edu.washington.escience.util.UrlNormalizer;
import edu.washington.escience.util.WholeFileInputFormat;

/**
 * Produce output lines of the form: url_id url
 * 
 * The url_id is a simple hash of the url
 */
public class WarcVertices extends Configured implements Tool {

	static enum CounterTypes {BAD_SOURCE_URL};
	
	public static class Map extends WarcMapper {
		
		@Override
		public void processRecord(WarcRecord record, Context context) {			
			String targetUri = record.header.warcTargetUriStr;        	
			if (targetUri == null)
				return;
			
			try {
				Text uriText = new Text();
				Text idText = new Text();
				String normalizedURL = UrlNormalizer.normalizeURLString(targetUri, null);
				
				uriText.set(normalizedURL);				
				idText.set(UrlNormalizer.URLStringToIDString(normalizedURL));
				
				context.write(idText, uriText);
			} catch (Exception e) {
				e.printStackTrace();
				context.getCounter(CounterTypes.BAD_SOURCE_URL).increment(1);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text id, Iterable<Text> uris, Context context)
				throws IOException, InterruptedException {
			for (Text uri : uris) {
				context.write(id, uri);
				return;
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String inputPath = "/scratch/warc_data/1000wb-00.warc.gz";
		String outputPath = "/scratch/warc_data/processed/vertexes";
		
		if (args.length >= 1)
			inputPath = args[0];
		if (args.length >= 2)
			outputPath = args[1];
		
		System.out.println("input: " + inputPath);
		System.out.println("output: " + outputPath);

	    Job job = new Job(getConf());
	    job.setJarByClass(WarcVertices.class);
	    
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(16);

		job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 :  1;
	}
	    	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WarcVertices(), args);
		System.exit(exitCode);
	}
}
