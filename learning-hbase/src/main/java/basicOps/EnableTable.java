package basicOps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class EnableTable {

	public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		// Get HbaseAdmin class
		Admin admin = connection.getAdmin();

		// Verifying weather the table is disabled
		Boolean bool = admin.isTableEnabled(TableName.valueOf("emp"));
		System.out.println(bool);

		// Disabling the table using HBaseAdmin object
		if (!bool) {
			admin.enableTable(TableName.valueOf("emp"));
			System.out.println("Table Enabled");
		}

	}

}
