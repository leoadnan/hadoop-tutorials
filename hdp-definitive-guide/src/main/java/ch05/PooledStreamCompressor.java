package ch05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class PooledStreamCompressor {

	public static void main(String[] args) {
		String codecClassName = args[0];
		
		Configuration conf = new Configuration();
		Compressor compressor = null;
		
		try {
			Class<?> codecClass = Class.forName(codecClassName);
			CompressionCodec codec=  (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf); 
			
			compressor = CodecPool.getCompressor(codec);
			CompressionOutputStream out = codec.createOutputStream(System.out, compressor);
			
			IOUtils.copyBytes(System.in, out, conf);
			out.finish();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			CodecPool.returnCompressor(compressor);
		}
	}

}
