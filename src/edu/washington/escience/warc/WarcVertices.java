package edu.washington.escience.warc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import edu.washington.escience.util.UrlNormalizer;
import edu.washington.escience.util.WholeFileInputFormat;

/**
 * Produce output lines of the form: url_id url
 * 
 * The url_id is a simple hash of the url
 */
public class WarcVertices extends Configured implements Tool {

	public static class Map extends Mapper<NullWritable, BytesWritable, Text, Text > {
		
		@Override
		public void map(NullWritable key, BytesWritable value, Context context) 
				throws IOException, InterruptedException {
			ByteArrayInputStream bis = new ByteArrayInputStream(value.getBytes());
			Text uriText = new Text();
			Text idText = new Text();
			
			// Work around broken gzip decoder in jwat
			InputStream in = new GZIPInputStream(bis);
			
			WarcReader reader = WarcReaderFactory.getReaderUncompressed(in);
			WarcRecord record;
			
			try {
				while ( (record = reader.getNextRecord()) != null ) {
					String targetUri = record.header.warcTargetUriStr;
		        	
					if (targetUri == null)
						continue;
				
					// normalize the URI by lower-casing, stripping fragment, etc.
					String normalizedURL = UrlNormalizer.normalizeURLString(targetUri, null);
					uriText.set(normalizedURL);
				
					// calculate an "id", which is just part of the sha-1 hash of the URI
					idText.set(UrlNormalizer.URLStringToIDString(normalizedURL));
					context.write(idText, uriText);				
				}
			} catch(Exception e) {
				e.printStackTrace();
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
