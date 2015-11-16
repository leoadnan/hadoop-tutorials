package ch03.get;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class Ex3_GetCheckExistenceExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		List<Put> puts = new ArrayList<Put>();
		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		puts.add(put1);
		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
		puts.add(put2);
		Put put3 = new Put(Bytes.toBytes("row2"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"));
		puts.add(put3);
		
		// Puts Insert two rows into the table.
		table.put(puts);

		Get get1 = new Get(Bytes.toBytes("row2"));
		get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		get1.setCheckExistenceOnly(true);
		
		// Get1 Check first with existing data.
		Result result1 = table.get(get1);
		byte[] val = result1.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		System.out.println("Get 1 Exists: " + result1.getExists());
		// Result1 Exists is "true", while no cel was actually returned.
		System.out.println("Get 1 Size: " + result1.size());
		System.out.println("Get 1 Value: " + Bytes.toString(val));

		Get get2 = new Get(Bytes.toBytes("row2"));
		// 4-Get2 Check for an entire family to exist.
		get2.addFamily(Bytes.toBytes("colfam1"));
		get2.setCheckExistenceOnly(true);
		Result result2 = table.get(get2);
		System.out.println("Get 2 Exists: " + result2.getExists());
		System.out.println("Get 2 Size: " + result2.size());

		Get get3 = new Get(Bytes.toBytes("row2"));
		// Get3 Check for a non-existent column.
		get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999"));
		get3.setCheckExistenceOnly(true);
		Result result3 = table.get(get3);
		System.out.println("Get 3 Exists: " + result3.getExists());
		System.out.println("Get 3 Size: " + result3.size());

		Get get4 = new Get(Bytes.toBytes("row2"));
		// Get4 Check for an existent, and non-existent column.
		get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999"));
		get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		get4.setCheckExistenceOnly(true);
		Result result4 = table.get(get4);
		// Result4 Exists is "true" because some data exists.
		System.out.println("Get 4 Exists: " + result4.getExists());
		System.out.println("Get 4 Size: " + result4.size());

		table.close();
		connection.close();
		helper.close();
	}
}
