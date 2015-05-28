package ch03;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {

	public static void main(String args[]){
		String file ="hdfs://master:9000/user/aahmed/quangle.txt";
		
		Configuration conf = new Configuration();
		InputStream in=null;
		try {
			FileSystem fs = FileSystem.get(URI.create(file), conf);
			in=fs.open(new Path(file));
			IOUtils.copyBytes(in, System.out, 1024, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
