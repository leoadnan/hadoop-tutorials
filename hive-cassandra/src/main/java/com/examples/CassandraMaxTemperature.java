package com.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.SimpleFormatter;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CassandraMaxTemperature {

	static class TemperatureMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable> {

		private static final int MISSING = 9999;

		/* Mapper Class */
		public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, Context context) throws IOException, InterruptedException {

			String sensorid="";
			String recorddatatime="";
			String datastring="";
			
			//Get the values of the key column
			for(Entry<String, ByteBuffer> key : keys.entrySet()){
				
				//get the sensord key column
				if("sensorid".equalsIgnoreCase(key.getKey())){
					sensorid=ByteBufferUtil.string(key.getValue());
				}
				
				//get the recorddatetime key column
				if("recorddatetime".equalsIgnoreCase(key.getKey())){
					long lTick = ByteBufferUtil.toLong(key.getValue());
					DateFormat formatter = new SimpleDateFormat("d-MMM-yyyy-HH");
					recorddatatime = formatter.format(new Date(lTick));
				}
			}
			
			//Get the values of the value columns
			for(Entry<String, ByteBuffer> key: columns.entrySet()){
				//Get the datastring key column
				if("datastring".equalsIgnoreCase(key.getKey())){
					datastring = ByteBufferUtil.string(key.getValue());
				}
			}
			
			context.write(new Text(sensorid), new IntWritable(Integer.parseInt(datastring)));
			
		}
	}

	/* Reducer Class */
	static class TemperatureReducer extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int maxValue = Integer.MIN_VALUE;

			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			
			//write to cassandra
			//1. prepare the insert keys collection
			Map<String,ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
			keys.put("key", ByteBufferUtil.bytes(key.toString()));
			
			//2.Prepare the insert variables collection
			//There must be one value here for each of the ? in the CQL update statement
			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			variables.add(ByteBufferUtil.bytes(maxValue));
			
			context.write(keys, variables);
		}
	}

	/* Main Class */
	public static void main(String[] args) throws Exception {

//		if (args.length != 2) {
//			System.err.println("Usage: NewMaxTemperature <input path> <output path>");
//			System.exit(-1);
//		}

		Job job = new Job();
		job.setJarByClass(CassandraMaxTemperature.class);

		//Setup Cassandra Input
		ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), "test", "sensorlogs");
		ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
		CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
		job.setInputFormatClass(CqlInputFormat.class);
		
		//Setup Cassandra Output
		ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
		String query="update test.max_temp set datastring=?";
		CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
		ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
		ConfigHelper.setOutputKeyspace(job.getConfiguration(), "test");
		job.setOutputFormatClass(CqlOutputFormat.class);
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		job.setMapperClass(TemperatureMapper.class);
//		job.setReducerClass(TemperatureReducer.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
