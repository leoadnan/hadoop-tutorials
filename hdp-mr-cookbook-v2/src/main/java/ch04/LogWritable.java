package ch04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LogWritable implements WritableComparable<LogWritable> {

	private Text userIP, timestamp, request;
	private IntWritable responseSize, status;

	public LogWritable() {
		this.userIP = new Text();
		this.timestamp = new Text();
		this.request = new Text();
		this.responseSize = new IntWritable();
		this.status = new IntWritable();
	}

	public void set(String userIP, String timestamp, String request, int bytes, int status) {
		this.userIP.set(userIP);
		this.timestamp.set(timestamp);
		this.request.set(request);
		this.responseSize.set(bytes);
		this.status.set(status);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		userIP.readFields(in);
		timestamp.readFields(in);
		request.readFields(in);
		responseSize.readFields(in);
		status.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		userIP.write(out);
		timestamp.write(out);
		request.write(out);
		responseSize.write(out);
		status.write(out);
	}

	@Override
	public int compareTo(LogWritable o) {
		if (userIP.compareTo(o.userIP) == 0) {
			return timestamp.compareTo(o.timestamp);
		} else
			return userIP.compareTo(o.userIP);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userIP == null) ? 0 : userIP.hashCode());
		return result;
	}

	public boolean equals(Object o) {
		if (o instanceof LogWritable) {
			LogWritable other = (LogWritable) o;
			return userIP.equals(other.userIP) && timestamp.equals(other.timestamp);
		}
		return false;
	}

	@Override
	public String toString() {
		return  userIP + "\t" + timestamp + "\t" + request + "\t" + status + "\t" + responseSize;
	}

	public Text getUserIP() {
		return userIP;
	}

	public Text getTimestamp() {
		return timestamp;
	}

	public Text getRequest() {
		return request;
	}

	public IntWritable getResponseSize() {
		return responseSize;
	}

	public IntWritable getStatus() {
		return status;
	}
}
