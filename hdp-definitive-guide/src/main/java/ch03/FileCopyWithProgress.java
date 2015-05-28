package ch03;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	public static void main(String args[]){
		String srcPath="/Users/aahmed/Documents/Books-Code/hadoop-book-master/input/ncdc/1901";
		String destPath="hdfs://master:9000/user/aahmed/1901_"+System.currentTimeMillis();
		
		Configuration conf= new Configuration();
		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(srcPath), 4096);
			FileSystem fs = FileSystem.get(URI.create(destPath), conf);
			OutputStream out = fs.create(new Path(destPath), new Progressable() {
				
				@Override
				public void progress() {
					System.out.print(".");
					
				}
			});
			
			IOUtils.copyBytes(bis, out, 4096,true);

			IOUtils.closeStream(out);
			IOUtils.closeStream(bis);
			fs.setReplication(new Path(destPath), (short)1);
			
			FileStatus fStatus = fs.getFileStatus(new Path(destPath));
			System.out.println(fStatus.getBlockSize());
			System.out.println(fStatus.getOwner());
			System.out.println(fStatus.getReplication());
			System.out.println(fStatus.getLen());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
