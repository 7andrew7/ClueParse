package edu.washington.escience.warc;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import edu.washington.escience.util.UrlNormalizer;

public abstract class WarcMapper extends Mapper<LongWritable, ClueWarcRecord, Text, Text> {

	static enum CounterTypes { PAGES, ERROR_MISSING_TARGET, ERROR_PAGE_TOO_LARGE, ERROR_BAD_SOURCE_URL };
	
	private static final int LARGE_PAGE_SIZE = 4 * 1024*1024;

	@Override
	public void map(LongWritable key, ClueWarcRecord record, Context context) 
			throws IOException, InterruptedException {
		String targetUrlStr = record.getHeaderMetadataItem("WARC-Target-URI");       	
		if (targetUrlStr == null) {
			context.getCounter(CounterTypes.ERROR_MISSING_TARGET).increment(1);
			return;			
		}
		
		byte[] content = record.getByteContent();
		
		 // Large pages cause jsoup to go out to lunch...
		if (content.length > LARGE_PAGE_SIZE) {
			context.getCounter(CounterTypes.ERROR_PAGE_TOO_LARGE).increment(1);
			return;
		}

		context.getCounter(CounterTypes.PAGES).increment(1);
		
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
			context.getCounter(CounterTypes.ERROR_BAD_SOURCE_URL).increment(1);
			return;
		}
		
		processRecord(content, contextURL, normalizedSourceUrlStr, sourceHash, context);
	}
	
	/**
	 * Process a WARC record
	 * 
	 * @param content: The content of the WARC record as a byte array
	 * @param sourceURL: The URL of the record
	 * @param normalizedSourceUrlStr: A normalized version of the URL, in string form
	 * @param sourceUrlStrHash: A hashed version of the normalized URL
	 */
	public abstract void processRecord(byte []content, URL sourceURL, String normalizedSourceUrlStr,
			String sourceUrlStrHash, Context context) throws IOException, InterruptedException;
}
