package edu.washington.escience.warc;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class HtmlParseTest {
	public static void main(String [] args) {
		String html = "<html><head><title>First parse</title></head>"
				  + "<body><p>Parsed HTML into a doc.</p>" +
				  	"<a href=\"joe.com\">foo</a>" +
				  	"<a href=\"zzz.com\"  > slkdfjsd   </A>" +
				  	"</body></html>";
		
		Document doc = Jsoup.parse(html);
		Elements links = doc.select("a[href]"); // a with href
		for (Element link : links) {
			System.out.println(link.attr("href"));
		}
	}
}
