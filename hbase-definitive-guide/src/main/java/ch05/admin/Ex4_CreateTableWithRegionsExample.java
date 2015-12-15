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
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import util.HBaseHelper;

//Example using the administrative API to create a table with predefined regions
public class Ex4_CreateTableWithRegionsExample {

	private static Configuration conf = null;
	private static Connection connection = null;

	// 1-PrintTable Helper method to print the regions of a table.
	private static void printTableRegions(String tableName) throws IOException {
		System.out.println("Printing regions of table: " + tableName);
		TableName tn = TableName.valueOf(tableName);
		RegionLocator locator = connection.getRegionLocator(tn);
		
		// 2-GetKeys Retrieve the start and end keys from the newly created table.
		Pair<byte[][], byte[][]> pair = locator.getStartEndKeys();
		for (int n = 0; n < pair.getFirst().length; n++) {
			byte[] sk = pair.getFirst()[n];
			byte[] ek = pair.getSecond()[n];
			// 3-Print Print the key, but guarding against the empty start (and end) key.
			System.out.println("[" + (n + 1) + "]"
					+ " start key: " + (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk))
					+ ", end key: " + (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
		}
		locator.close();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		conf = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(conf);

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable1");
		helper.dropTable("testtable2");

		Admin admin = connection.getAdmin();

		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("testtable1"));
		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
		desc.addFamily(coldef);

		// 4-CreateTable1 Call the createTable() method while also specifying the region boundaries.
		admin.createTable(desc, Bytes.toBytes(1L), Bytes.toBytes(100L), 10);
		printTableRegions("testtable1");

		// 5-Regions Manually create region split keys.
		byte[][] regions = new byte[][] { 
				Bytes.toBytes("A"),
				Bytes.toBytes("D"), 
				Bytes.toBytes("G"), 
				Bytes.toBytes("K"),
				Bytes.toBytes("O"), 
				Bytes.toBytes("T") };
		HTableDescriptor desc2 = new HTableDescriptor(TableName.valueOf("testtable2"));
		desc2.addFamily(coldef);
		
		// 6-CreateTable2 Call the crateTable() method again, with a new table name and the list of region split keys.
		admin.createTable(desc2, regions);
		printTableRegions("testtable2");
	}

}
