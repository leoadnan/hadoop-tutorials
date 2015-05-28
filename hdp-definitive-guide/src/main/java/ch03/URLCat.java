package ch03;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class URLCat {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) throws MalformedURLException, IOException {
		InputStream in=null;
		try {
			in =new URL("hdfs://master:9000/user/aahmed/sample.txt").openStream();
			IOUtils.copyBytes(in, System.out, 1024, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
