package cassawordcount;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class CassaWordCountReducer extends 
        Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        List<Mutation> columnsToAdd = new ArrayList<Mutation>();
        Integer wordCount = 0;
        for(IntWritable value : values) {
            wordCount += value.get();
        }
//        Column countCol = new Column(ByteBuffer.wrap("w_count".getBytes()));
//        countCol.setValue(ByteBuffer.wrap(wordCount.toString().getBytes()));
//        countCol.setTimestamp(new Date().getTime());
//        
//        ColumnOrSuperColumn wordCosc = new ColumnOrSuperColumn();
//        wordCosc.setColumn(countCol);
//        
//        Mutation countMut = new Mutation();
//        countMut.column_or_supercolumn = wordCosc;
//            
//        columnsToAdd.add(countMut);
//        context.write(ByteBuffer.wrap(key.toString().getBytes()), columnsToAdd);
        
		Column wordcol = new Column(ByteBuffer.wrap("word".getBytes()));
		wordcol.setValue(ByteBuffer.wrap(key.toString().getBytes()));
		wordcol.setTimestamp(new Date().getTime());

		
		Column countCol = new Column(ByteBuffer.wrap("w_count".getBytes()));
		countCol.setValue(ByteBuffer.wrap(wordCount.toString().getBytes()));
		countCol.setTimestamp(new Date().getTime());

		ColumnOrSuperColumn wordCosc = new ColumnOrSuperColumn();
		wordCosc.setColumn(countCol);
		
		ColumnOrSuperColumn wordCountCosc = new ColumnOrSuperColumn();
		wordCountCosc.setColumn(wordcol);

		Mutation countMut = new Mutation();
		countMut.column_or_supercolumn = wordCosc;

		Mutation wordMut = new Mutation();
		countMut.column_or_supercolumn = wordCountCosc;

		columnsToAdd.add(countMut);
		columnsToAdd.add(wordMut);
		context.write(ByteBuffer.wrap(key.toString().getBytes()), columnsToAdd);

    }
    
    private ByteBuffer longToByteBuffer(long number){
        byte b[] = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap(b);
        buf.putLong(number);
        return buf;
    }
}
