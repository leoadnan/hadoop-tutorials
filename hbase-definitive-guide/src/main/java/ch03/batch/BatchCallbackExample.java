package ch03.batch;

//Example application using batch operations with callbacks
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class BatchCallbackExample {

	private final static byte[] ROW1 = Bytes.toBytes("row1");
	private final static byte[] ROW2 = Bytes.toBytes("row2");
	private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
	private final static byte[] COLFAM2 = Bytes.toBytes("colfam2");
	private final static byte[] QUAL1 = Bytes.toBytes("qual1");
	private final static byte[] QUAL2 = Bytes.toBytes("qual2");

	public static void main(String[] args) throws IOException,
			InterruptedException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1", "colfam2");
	    helper.put("testtable",
	    	      new String[] { "row1" },
	    	      new String[] { "colfam1" },
	    	      new String[] { "qual1", "qual2", "qual3" },
	    	      new long[] { 1, 2, 3 },
	    	      new String[] { "val1", "val2", "val3" });
		System.out.println("Before batch call...");
		helper.dump("testtable", new String[] { "row1", "row2" }, null, null);

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		// 2- Create a list to hold all values.
		List<Row> batch = new ArrayList<Row>();

		Put put = new Put(ROW2);
		// 3- Add a Put instance.
		put.addColumn(COLFAM2, QUAL1, 4, Bytes.toBytes("val5"));
		batch.add(put);

		Get get1 = new Get(ROW1);
		// 4- Add a Get instance for a different row.
		get1.addColumn(COLFAM1, QUAL1);
		batch.add(get1);

		Delete delete = new Delete(ROW1);
		// 5- Add a Delete instance.
		delete.addColumns(COLFAM1, QUAL2);
		batch.add(delete);

		Get get2 = new Get(ROW2);
		// 6- Add a Get instance that will fail.
		get2.addFamily(Bytes.toBytes("BOGUS"));
		batch.add(get2);

		// 7- Create result array.
		Object[] results = new Object[batch.size()];
		try {
			table.batchCallback(batch, results, new Batch.Callback<Result>() {
				@Override
				public void update(byte[] region, byte[] row, Result result) {
					System.out.println("Received callback for row["+ Bytes.toString(row) + "] -> " + result);
				}
			});
		} catch (Exception e) {
			// 8-Print Print error that was caught.
			System.err.println("Error: " + e);
		}

		for (int i = 0; i < results.length; i++) {
			// 9-Dump Print all results and class types.
			System.out.println("Result[" + i + "]: type = "
							+ results[i].getClass().getSimpleName() + "; "
							+ results[i]);
		}

		table.close();
		connection.close();
		System.out.println("After batch call...");
		helper.dump("testtable", new String[] { "row1", "row2" }, null, null);
		helper.close();
	}
}
