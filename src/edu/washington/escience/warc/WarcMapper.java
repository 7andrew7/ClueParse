package edu.washington.escience.warc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;


public abstract class WarcMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

	static enum WarcMapperError { RECORD_READ_FAILURE };
	
	@Override
	public void map(NullWritable key, BytesWritable value, Context context) 
			throws IOException, InterruptedException {
		ByteArrayInputStream bis = new ByteArrayInputStream(value.getBytes());
		
		// Work around broken gzip decoder in jwat
		InputStream in = new GZIPInputStream(bis);
		
		WarcReader reader = WarcReaderFactory.getReaderUncompressed(in);
		WarcRecord record = null;
		
		while (true) {
			try {
				record = reader.getNextRecord();
				if (record == null)
					return;
				processRecord(record, context);
			} catch (Exception ex) {
				ex.printStackTrace();
				context.getCounter(WarcMapperError.RECORD_READ_FAILURE).increment(1);
				continue;
			}
		}
	}
	
	public abstract void processRecord(WarcRecord record, Context context);
}
