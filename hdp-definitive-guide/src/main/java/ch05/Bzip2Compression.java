package ch05;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class Bzip2Compression {

	public static void main(String[] args) {
		String inputFile = "/Users/aahmed/Documents/Books-Code/hadoop-book-master/input/ncdc/large.txt";
		String outFile = "hdfs://master:9000/user/aahmed/";
		
		File file = new File(inputFile);
		outFile = outFile+file.getName()+".bz2";
		
		Configuration conf = new Configuration();
		BufferedInputStream  in = null;
		CompressionOutputStream out = null;
		try {
			in = new BufferedInputStream(new FileInputStream(new File(inputFile)), 16384);
			
			FileSystem fs = FileSystem.get(URI.create(outFile),conf);
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(new Path(outFile));
			
			out = codec.createOutputStream(fs.create(new Path(outFile)));
			
			IOUtils.copyBytes(in, out, 16384);
			out.finish();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}

}
