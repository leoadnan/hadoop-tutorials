package ch03.put;

// Special error handling with lists of puts
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class Ex10_PutListErrorExample3 {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		try (HBaseHelper helper = HBaseHelper.getHelper(conf);
				Connection connection = ConnectionFactory.createConnection(conf);
				Table table = connection.getTable(TableName.valueOf("testtable"));) {
			
			helper.dropTable("testtable");
			helper.createTable("testtable", "colfam1");
			
			List<Put> puts = new ArrayList<Put>();

			Put put1 = new Put(Bytes.toBytes("row1"));
			put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
			puts.add(put1);
			
			Put put2 = new Put(Bytes.toBytes("row2"));
			put2.addColumn(Bytes.toBytes("BOGUS"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
			puts.add(put2);
			
			Put put3 = new Put(Bytes.toBytes("row3"));
			put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
			puts.add(put3);

			Put put4 = new Put(Bytes.toBytes("row4"));
			put4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
			puts.add(put4);

			try {
				// 1- Store multiple rows with columns into HBase.
				table.put(puts);
			} catch (RetriesExhaustedWithDetailsException e) {
				// 2- Error Handle failed operations.
				int numErrors = e.getNumExceptions();
				System.out.println("Number of exceptions: " + numErrors);
				for (int n = 0; n < numErrors; n++) {
					System.out.println("Cause[" + n + "]: " + e.getCause(n));
					System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
					// 3- ErrorPut Gain access to the failed operation.
					System.out.println("Row[" + n + "]: " + e.getRow(n));
				}
				System.out.println("Cluster issues: " + e.mayHaveClusterIssues());
				System.out.println("Description: " + e.getExhaustiveDescription());
			}
		}
	}
}
