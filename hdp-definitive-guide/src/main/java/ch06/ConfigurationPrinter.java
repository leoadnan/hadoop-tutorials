package ch06;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ConfigurationPrinter extends Configured implements Tool {

	static {
//		Configuration.addDefaultResource("hdfs-default.xml");
//		Configuration.addDefaultResource("hdfs-site.xml");
//		Configuration.addDefaultResource("yarn-default.xml");
//		Configuration.addDefaultResource("yarn-site.xml");
//		Configuration.addDefaultResource("mapred-default.xml");
//		Configuration.addDefaultResource("mapred-site.xml");
	}
	
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.addResource("configuration-1.xml");
		for(Entry<String, String> e:conf){
			System.out.printf("%s=%s\n", e.getKey(), e.getValue());
		}
		
		System.out.println(conf.get("color"));

//		Properties p = System.getProperties();
//		for (Entry<Object, Object> entry : p.entrySet()) {
//			System.out.println(entry.getKey()+"="+entry.getValue());
//		}
		
//		System.out.println(System.getProperty("TEST"));
		System.out.println(conf.get("size"));
		System.out.println(conf.get("size-weight"));
		System.out.println(conf.get("breadth", "wide"));
		return 0;
	}

}
