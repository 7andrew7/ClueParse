package edu.washington.escience.warc;

import java.io.*;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.common.Payload;
import org.jwat.warc.*;

public final class WarcParser {
	private static final String warcFile = "/scratch/warc_data/1000wb-00.warc.gz";
	
	private static int countLinks(InputStream is) throws Exception {
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
        
        // work around badness in jwat's compression handler
        InputStream fin = new FileInputStream(file);
        InputStream in = fin;
        if (warcFile.endsWith(".gz"))
        	in = new GZIPInputStream(fin);
        
        int records = 0;
        int links = 0;
        
        WarcReader reader = WarcReaderFactory.getReaderUncompressed(in);
        WarcRecord record;
        while ( (record = reader.getNextRecord()) != null ) {
        	String targetUri = record.header.warcTargetUriStr;
        	
        	if (targetUri == null)
        		continue;
        	        	
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

}
