package edu.washington.escience.warc;

import java.io.*;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.warc.*;

public class WarcParser extends Configured implements Tool {
	private static final String warcFile = "/scratch/warc_data/1000wb-00.warc";
	
	private static int countLinks(InputStream is) throws Exception {
		// XXX
		int count = 0;
		String str = IOUtils.toString(is, "UTF-8");
		
		Document doc = Jsoup.parse(str);
		Elements links = doc.select("a[href]"); // a with href
		for (Element link : links) {
			count++;
		}	
		
		return count;
	}
	
    public static void main(String[] args) throws Exception {
        File file = new File(warcFile);
        
        InputStream in = new FileInputStream( file );
 
        int records = 0;
        int links = 0;
        
        WarcReader reader = WarcReaderFactory.getReader( in );
        WarcRecord record;
        while ( (record = reader.getNextRecord()) != null ) {
        	String targetUri = record.header.warcTargetUriStr;
        	
        	if (targetUri == null)
        		continue;
        	
        	HttpHeader header = record.getHttpHeader();
        	
        	Payload payload = record.getPayload();
        	if (payload != null) {
        		InputStream is = payload.getInputStream();
        		links += countLinks(is);
        	}
        	
        	++records;                      	
        }
 
        System.out.println(records + " " + links);
        reader.close();
        in.close();
    }

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
