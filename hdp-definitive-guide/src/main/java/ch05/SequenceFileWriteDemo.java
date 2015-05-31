package ch05;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.iq80.leveldb.CompressionType;

public class SequenceFileWriteDemo {

	private static final String[] DATA = {
		"One, two, buckle my shoe",
		"Three, four, shut the door",
		"Five, six, pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, a big fat hen"
	};
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		String output = args[0];
		
		Configuration conf = new Configuration();
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(output), conf);
			Path p = new Path(output);

			if(fs.exists(p)){
				fs.delete(p, true);
			}
//			SequenceFile.CompressionType compressionType=SequenceFile.CompressionType.BLOCK;
//			CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
//			CompressionCodec codec = ccf.getCodecByClassName(DefaultCodec.class.getName());
//			SequenceFile.setDefaultCompressionType(conf, SequenceFile.CompressionType.BLOCK);		
//			writer= SequenceFile.createWriter(fs, conf, p, key.getClass(), value.getClass(), compressionType, codec);
			
			writer= SequenceFile.createWriter(fs, conf, p, key.getClass(), value.getClass());
			for (int i=0; i<100; i++){
				key.set(100-i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			IOUtils.closeStream(writer);
		}
	}

}
