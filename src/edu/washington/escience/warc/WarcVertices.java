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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import edu.washington.escience.util.WholeFileInputFormat;

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
			while ( (record = reader.getNextRecord()) != null ) {
				String targetUri = record.header.warcTargetUriStr;
		        	
				if (targetUri == null)
					continue;
				
				// normalize the URI by lower-casing, stripping fragment, etc.
				String normalizedURL = Util.normalizeURLString(targetUri, null);
				uriText.set(normalizedURL);
				
				// calculate an "id", which is just part of the sha-1 hash of the URI
				idText.set(Util.URLStringToIDString(normalizedURL));
				context.write(idText, uriText);
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
	    job.setJarByClass(WarcVertices.class);
	    
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
		int exitCode = ToolRunner.run(new WarcVertices(), args);
		System.exit(exitCode);
	}
}
