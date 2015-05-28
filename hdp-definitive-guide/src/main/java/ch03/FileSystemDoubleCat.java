package ch03;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemDoubleCat {

	public static void main(String args[]){
		String filePath="hdfs://master:9000/user/aahmed/quangle.txt";
		
		Configuration conf = new Configuration();
		FSDataInputStream in=null;
		try {
			FileSystem fs = FileSystem.get(URI.create(filePath), conf);
			in = fs.open(new Path(filePath));
			IOUtils.copyBytes(in, System.out, 4096);
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 4096);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
