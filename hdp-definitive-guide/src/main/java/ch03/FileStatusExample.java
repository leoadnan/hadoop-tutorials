package ch03;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileStatusExample {
	public static void main(String args[]){
		String destPath="/airline";
		
		Configuration conf= new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(destPath), conf);
			
			FileStatus[] fStatus = fs.listStatus(new Path(destPath));
			for (FileStatus fileStatus : fStatus) {
				System.out.print(fileStatus.getPath().getName()+"\t");
				System.out.print(fileStatus.getBlockSize() +"\t");
				System.out.print(fileStatus.getOwner()+"\t");
				System.out.print(fileStatus.getReplication()+"\t");
				System.out.println(fileStatus.getLen());				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
