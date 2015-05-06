package com.examples.ch02;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author aahmed
 */
public class WeblogMapper_Ex_5 extends Mapper<LongWritable, Text, AvroWrapper, NullWritable> {

    private AvroWrapper<WeblogRecord> outputRecord = new AvroWrapper<WeblogRecord>();

    private WeblogRecord weblogRecord = new WeblogRecord();
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        String cookie = tokens[0];
        String page = tokens[1];
        String date = tokens[2];
        String time = tokens[3];
        String formattedDate = date + ":" + time;
        Date timestamp = null;
        try {
            timestamp = dateFormatter.parse(formattedDate);
        } catch (ParseException ex) {
            return;
        }
        String ip = tokens[4];
        weblogRecord.setCookie(cookie);
        weblogRecord.setDate(new java.sql.Date(timestamp.getTime()));
        weblogRecord.setIp(ip);
        weblogRecord.setPage(page);
        
        outputRecord.datum(weblogRecord);
        context.write(outputRecord, NullWritable.get());
    }

}
