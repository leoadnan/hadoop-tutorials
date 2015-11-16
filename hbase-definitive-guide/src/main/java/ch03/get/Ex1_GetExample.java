package ch03.get;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class Ex1_GetExample {

	public static void main(String[] args) throws IOException {

		//1- Create the configuration.
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		if (!helper.existsTable("testtable")) {
			helper.createTable("testtable", "colfam1");
		}

		//2- Instantiate a new table reference.
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		//3- Create ch03.get with specific row.
		Get get = new Get(Bytes.toBytes("row1"));

		//4- Add a column to the ch03.get.
		 get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

		//5- Retrieve row with selected columns from HBase.
		Result result = table.get(get);
		
		//6- Get a specific value for the given column.
		byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

		//7- Print out the value while converting it back.
		System.out.println("Value: " + Bytes.toString(val));

		//8- Close the table and connection instances to free resources.
		table.close();
		connection.close();

		helper.close();
	}
}
