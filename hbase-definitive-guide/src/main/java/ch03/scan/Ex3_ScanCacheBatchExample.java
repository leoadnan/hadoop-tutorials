package ch03.scan;

//Example using caching and batch parameters for scans
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
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import util.HBaseHelper;

public class Ex3_ScanCacheBatchExample {

	private static Table table = null;

	private static void scan(int caching, int batch, boolean small)
			throws IOException {
		int count = 0;
		Scan scan = new Scan()
			.setCaching(caching) //1-Set Set caching and batch parameters.
			.setBatch(batch)
			.setSmall(small)
			.setScanMetricsEnabled(true);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			count++; // 2- Count the number of Results available.
		}
		scanner.close();
		ScanMetrics metrics = scan.getScanMetrics();
		System.out.println("Caching: " + caching + 
						", Batch: " + batch + 
						", Small: " + small + 
						", Results: " + count + 
						", RPCs: " + metrics.countOfRPCcalls);
	}

	public static void main(String[] args) throws IOException {
		// ^^ Ex3_ScanCacheBatchExample
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2");
		helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

		Connection connection = ConnectionFactory.createConnection(conf);
		table = connection.getTable(TableName.valueOf("testtable"));

		scan(1, 1, false);
		scan(1, 0, false);
		scan(1, 0, true);
		scan(200, 1, false);
		scan(200, 0, false);
		scan(200, 0, true);
		scan(2000, 100, false); //3-Test Test various combinations.
		scan(2, 100, false);
		scan(2, 10, false);
		scan(5, 100, false);
		scan(5, 20, false);
		scan(10, 10, false);
		
		table.close();
		connection.close();
		helper.close();
	}
}
