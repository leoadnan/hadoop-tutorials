package ch03;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CoherencyModelTest {
	public static void main(String args[]){
		Configuration conf = new Configuration();
		
		String file ="hdfs://master:9000/user/aahmed/file";
		Path p = new Path(file);
		
		try {
			FileSystem fs = FileSystem.get(URI.create(file), conf);
			if (fs.exists(p)){
				fs.delete(p, true);
			}
			FSDataOutputStream out = fs.create(p);
			System.out.println("is exists: "+fs.exists(p));

			out.write("content".getBytes("UTF-8"));
			out.hflush();
			System.out.println("file length before closing: "+fs.getFileStatus(p).getLen());
			out.close();
			System.out.println("file length after closing: "+fs.getFileStatus(p).getLen());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
