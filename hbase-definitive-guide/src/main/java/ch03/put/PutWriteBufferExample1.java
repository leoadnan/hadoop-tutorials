package ch03.put;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class PutWriteBufferExample1 {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");
		TableName name = TableName.valueOf("testtable");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(name);

		// CheckFlush Get a mutator instance for the table.
		BufferedMutator mutator = connection.getBufferedMutator(name);

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

		// Put Store some rows with columns into HBase.
		mutator.mutate(put1);

		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
		mutator.mutate(put2);

		Put put3 = new Put(Bytes.toBytes("row3"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
		mutator.mutate(put3);

		Get get = new Get(Bytes.toBytes("row1"));
		Result res1 = table.get(get);

		// Get1 Try to load previously stored row, this will print "Result: keyvalues=NONE".
		System.out.println("Result: " + res1);

		// Flush Force a flush, this causes an RPC to occur.
		mutator.flush();

		Result res2 = table.get(get);
		// Now the row is persisted and can be loaded.
		System.out.println("Result: " + res2);

		mutator.close();
		table.close();
		connection.close();
		helper.close();
	}
}
