package edu.washington.escience.util;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;

/**
 * Utilities for normalizing and hashing URLs
 */
public final class UrlNormalizer {

	/**
	 * Normalize a String containing a URL:
	 * 
	 * 1) Expand relative URLs to absolute given the context argument.
	 * 2) Strip off the protocol (http, https)
	 * 3) Strip off the fragment: http://foo.com/bar?query#fragment
	 * 4) Lowercase the hostname.
	 */
	public static String normalizeURLString(String urlString, URL context) throws MalformedURLException {
		StringBuilder builder = new StringBuilder();
		URL url = new URL(context, urlString);
		builder.append(url.getHost().toLowerCase());
		
		if (url.getPort() != -1) {
			builder.append(":");
			builder.append(url.getPort());
		}
		
		builder.append(url.getFile());
		return builder.toString();
	}
	
	/**
	 * Calculate a unique ID for a normalized URL string.
	 */
	public static String URLStringToIDString(String urlString) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-1");
			digest.update(urlString.getBytes("UTF-8"));
			byte[] fullHash = digest.digest();
			
			// convert hash string to base64; only preserve first 12 characters == 72 bits
			String hashStr = Base64.encodeBase64String(fullHash);
			return hashStr.substring(0, 12);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return null; // not reached
	}
	
	public static void main(String []args) throws MalformedURLException {
		
		System.out.println(normalizeURLString("https://foo.bar.com/index.html?blah#sam", null));
		System.out.println(normalizeURLString("index.html?blah#sam", new URL("http://www.cs.washington.edu:8080/foo?bar#slouch")));
		System.out.println(normalizeURLString("#local", new URL("http://www.cs.washington.edu:8080/foo?bar#slouch")));
		System.out.println(normalizeURLString("http://cnn.com", null));
		System.out.println(normalizeURLString("http://cnn.com/index.html", null));
			
		System.out.println(URLStringToIDString("foo.bar.com/index.html?blah"));
		System.out.println(URLStringToIDString("A"));
		System.out.println(URLStringToIDString("foo.bar.com/index.html?blah"));
	}
}
