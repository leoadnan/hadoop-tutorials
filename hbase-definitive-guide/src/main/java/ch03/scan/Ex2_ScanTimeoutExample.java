package ch03.scan;

//Example timeout while using a scanner
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import util.HBaseHelper;

public class Ex2_ScanTimeoutExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2");
		System.out.println("Adding rows to table...");
		helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);

		// 1- Get currently configured lease timeout.
		int scannerTimeout = (int) conf.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1);
		System.out.println("Current (local) lease period: " + scannerTimeout + "ms");
		System.out.println("Sleeping now for " + (scannerTimeout + 5000) + "ms...");
		try {
			// 2- Sleep a little longer than the lease allows.
			Thread.sleep(scannerTimeout + 5000);
		} catch (InterruptedException e) {
			// ignore
		}
		System.out.println("Attempting to iterate over scanner...");
		while (true) {
			try {
				Result result = scanner.next();
				if (result == null)
					break;
				// 3-Dump Print row content.
				System.out.println(result);
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
		scanner.close();
		table.close();
		connection.close();
		helper.close();
	}
}
