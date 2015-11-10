package cassawordcount2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassaWordCountMapper extends 
        Mapper<LongWritable, Text, Text, IntWritable>  {

	private static final Logger logger = LoggerFactory.getLogger(CassaWordCountMapper.class);
	
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String s = value.toString();
        
        logger.debug("*******************************************"+s);
        for (String word : s.split("\\W+")) {
            if (word.length() > 0) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }  
}
