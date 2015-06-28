package ch06;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

	@Override
	@SuppressWarnings("unchecked")
	public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {

		// Log to stdout file
		System.out.println("STD_OUT_MESSAGE");
		System.out.println("Map key: " + key);

		System.err.println("STD_ERR_MESSAGE");

		// Log to syslog file
		LOG.info("SYS_LOG_MESSAGE");
		LOG.info("Map key: " + key);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Map value: " + value);
		}
		context.write((KEYOUT) key, (VALUEOUT) value);
	}
}
