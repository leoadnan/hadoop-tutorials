package ch07;

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

public class Ex2_PrintUserCount {

    public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("summary_user"));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result r;
        while (((r = scanner.next()) != null)) {
            byte[] key = r.getRow();
            int userId = Bytes.toInt(key);
            byte[] totalValue = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("total"));
            int count = Bytes.toInt(totalValue);
            System.out.println("key: " + userId+ ",  count: " + count);
        }
        scanner.close();
        table.close();
    }
}