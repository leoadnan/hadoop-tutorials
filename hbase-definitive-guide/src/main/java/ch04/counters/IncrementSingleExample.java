package ch04.counters;

//Example using the single counter increment methods
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class IncrementSingleExample {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "daily");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		// 1-Incr1 Increase counter by one.
		long cnt1 = table.incrementColumnValue(Bytes.toBytes("20110101"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);

		// 2-Incr2 Increase counter by one a second time.
		long cnt2 = table.incrementColumnValue(Bytes.toBytes("20110101"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);

		// 3-GetCurrent Get current value of the counter without increasing it.
		long current = table.incrementColumnValue(Bytes.toBytes("20110101"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), 0);

		// 4-Decr1 Decrease counter by one.
		long cnt3 = table.incrementColumnValue(Bytes.toBytes("20110101"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), -1);
		
		System.out.println("cnt1: " + cnt1 + 
					", cnt2: " + cnt2 + 
					", current: "+ current + 
					", cnt3: " + cnt3);
	}
}
