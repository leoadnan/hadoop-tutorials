/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.examples.ch03;

import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author aahmed
 */
public class CLFMapper_Ex_1 extends Mapper<Object, Text, Text, Text> {

    private SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
        private Pattern p = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\w+) (.+?) (.+?)\" (\\d+) (\\d+) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"", Pattern.DOTALL);

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String entry = value.toString();
        System.out.println(entry);
        Matcher m = p.matcher(entry);
        if (!m.matches()) {
            return;
        }
        Date date = null;
        try {
            date = dateFormatter.parse(m.group(4));
        } catch (ParseException ex) {
            return;
        }
        outputKey.set(m.group(1)); //ip
       
        StringBuilder b = new StringBuilder();
        b.append(date.getTime()); //timestamp
        b.append('\t');
        b.append(m.group(6)); //page
        b.append('\t');
        b.append(m.group(8)); //http status
        b.append('\t');
        b.append(m.group(9)); //bytes
        b.append('\t');
        b.append(m.group(12)); //useragent
        outputValue.set(b.toString());
        context.write(outputKey, outputValue);
    }
}
