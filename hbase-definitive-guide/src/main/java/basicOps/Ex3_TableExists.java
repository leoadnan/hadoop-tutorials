package basicOps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Ex3_TableExists {

	public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		// Get HbaseAdmin class
		Admin admin = connection.getAdmin();

		// Verifying the existance of the table
		boolean bool = admin.tableExists(TableName.valueOf("emp"));
		System.out.println(bool);
	}

}
