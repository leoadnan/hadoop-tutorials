package basicOps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Ex5_RetriveData {
	public static void main(String[] args) throws IOException, Exception {

		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		// Get HbaseAdmin class
		Admin admin = connection.getAdmin();

		// Instantiating HTable class
		Table table = connection.getTable(TableName.valueOf("emp"));

		// Instantiating Get class
		Get g = new Get(Bytes.toBytes("row1"));

		// Reading the data
		Result result = table.get(g);

		// Reading values from Result class object
		byte[] value = result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("name"));

		byte[] value1 = result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("city"));

		// Printing the values
		String name = Bytes.toString(value);
		String city = Bytes.toString(value1);

		System.out.println("name: " + name + " city: " + city);
	}
}
