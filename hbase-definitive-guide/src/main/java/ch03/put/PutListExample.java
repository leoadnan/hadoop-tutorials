package ch03.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class PutListExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		// CreateList Create a list that holds the Put instances.
		List<Put> puts = new ArrayList<Put>();

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		
		// AddPut1 Add ch03.put to list.
		puts.add(put1);

		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
		
		// AddPut2 Add another ch03.put to list.
		puts.add(put2);

		Put put3 = new Put(Bytes.toBytes("row2"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"));
		
		// AddPut3 Add third ch03.put to list.
		puts.add(put3);

		// Put Store multiple rows with columns into HBase.
		table.put(puts);
		table.close();
		connection.close();
		helper.close();
	}
}
