package ch03.scan;

// Example using a scanner to access data in a table
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class Ex1_ScanExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2");
		System.out.println("Adding rows to table...");
		// Tip: Remove comment below to enable padding, adjust start and stop
		// row, as well as columns below to match. See scan #5 comments.
		helper.fillTable("testtable", 1, 100, 100, 3, false, "colfam1", "colfam2");
//		helper.fillTable("testtable", 1, 100, 100, /*3, false,*/ "colfam1", "colfam2");
		
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		System.out.println("Scanning table #1...");
		// 1-NewScan Create empty Scan instance.
		Scan scan1 = new Scan();
		// 2-GetScanner Get a scanner to iterate over the rows.
		ResultScanner scanner1 = table.getScanner(scan1); 
		for (Result res : scanner1) {
			// 3-Dump Print row content.
			System.out.println(res);
		}
		// 4- Close scanner to free remote resources.
		scanner1.close();

		System.out.println("Scanning table #2...");
		Scan scan2 = new Scan();
		// 5- Add one column family only, this will suppress the retrieval of "colfam2".
		scan2.addFamily(Bytes.toBytes("colfam1"));
		ResultScanner scanner2 = table.getScanner(scan2);
		for (Result res : scanner2) {
			System.out.println(res);
		}
		scanner2.close();

		System.out.println("Scanning table #3...");
		Scan scan3 = new Scan();
		// 6- Use fluent pattern to add specific details to the Scan.
		scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-005"))
				.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col-033"))
				.setStartRow(Bytes.toBytes("row-010"))
				.setStopRow(Bytes.toBytes("row-020"));
		ResultScanner scanner3 = table.getScanner(scan3);
		for (Result res : scanner3) {
			System.out.println(res);
		}
		scanner3.close();

		System.out.println("Scanning table #4...");
		Scan scan4 = new Scan();
		scan4.setCaching(1);
		// 7-Build Only select one column.
		scan4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-005"))
				.setStartRow(Bytes.toBytes("row-010"))
				.setStopRow(Bytes.toBytes("row-020"));
		ResultScanner scanner4 = table.getScanner(scan4);
		for (Result res : scanner4) {
			System.out.println(res);
		}
		scanner4.close();

		System.out.println("Scanning table #5...");
		Scan scan5 = new Scan();
		// When using padding above, use "col-005", and "row-020", "row-010".
		scan5.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-005"))
				.setStartRow(Bytes.toBytes("row-020"))
				.setStopRow(Bytes.toBytes("row-010")).
				// 8-Build One column scan that runs in reverse.
				setReversed(true);
		ResultScanner scanner5 = table.getScanner(scan5);
		for (Result res : scanner5) {
			System.out.println(res);
		}
		scanner5.close();
		table.close();
		connection.close();
		helper.close();
	}
}
