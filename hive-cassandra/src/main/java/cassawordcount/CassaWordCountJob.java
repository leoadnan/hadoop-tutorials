package cassawordcount;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.hadoop.BulkOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;

public class CassaWordCountJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CassaWordCountJob(), args);
        System.exit(exitCode); 
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.printf("Usage: %s [generic options] <input dir>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Job job = Job.getInstance();
        job.setJobName(getClass().getName());
        job.setJarByClass(CassaWordCountJob.class);
        Config.setClientMode(true);
        
//        Configuration conf = job.getConfiguration();
        
        // cassandra bulk loader config
//        conf.set("cassandra.output.keyspace", "cassa_word_count");
//        conf.set("cassandra.output.columnfamily", "words");
        // OrderPreservingPartitioner for example could be used here too
//        conf.set("cassandra.output.partitioner.class", "org.apache.cassandra.dht.RandomPartitioner");
//        conf.set("cassandra.output.thrift.port","9160");    // default
//        conf.set("cassandra.output.thrift.address", "127.0.0.1");
//        conf.set("mapreduce.output.bulkoutputformat.streamthrottlembits", "400");

        
		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "cassa_word_count", "words");
		ConfigHelper.setOutputKeyspace(job.getConfiguration(), "cassa_word_count");
		ConfigHelper.setOutputRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
		job.getConfiguration().set("mapreduce.output.bulkoutputformat.buffersize", "64");
		job.getConfiguration().set("mapreduce.output.bulkoutputformat.localdir", "/Users/aahme25/sstable_data/");
		
        job.setMapperClass(CassaWordCountMapper.class);
        job.setReducerClass(CassaWordCountReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        job.setOutputFormatClass(BulkOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
