package ch05.admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

//Example using the administrative API to create a table
public class Ex2_CreateTableExample {

	public static void main(String[] args) throws IOException,
			InterruptedException {

		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");

		Connection connection = ConnectionFactory.createConnection(conf);
		// 1-CreateAdmin Create a administrative API instance.
		Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf("testtable");
		// 2-CreateHTD Create the table descriptor instance.
		HTableDescriptor desc = new HTableDescriptor(tableName);

		// 3-CreateHCD Create a column family descriptor and add it to the table descriptor.
		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
		desc.addFamily(coldef);

		// 4-CreateTable Call the createTable() method to do the actual work.
		admin.createTable(desc);

		// 5-Check Check if the table is available.
		boolean avail = admin.isTableAvailable(tableName);
		System.out.println("Table available: " + avail);

	}
}
