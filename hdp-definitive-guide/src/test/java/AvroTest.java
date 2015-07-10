import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import ch12.StringPair;

public class AvroTest {

	@Test
	public void testPairGeneric() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		Schema schema=parser.parse(getClass().getResourceAsStream("StringPair.avsc"));
		
		GenericRecord record = new GenericData.Record(schema);
		record.put("left", "L");
		record.put("right", "R");
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
//		OutputStream out =System.out;
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(record, encoder);
		
		encoder.flush();
		out.close();
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		GenericRecord result = reader.read(null, decoder);
		assertThat(result.get("left").toString(), is("L"));
	    assertThat(result.get("right").toString(), is("R"));
	}

	@Test
	public void testPairSpecific() throws IOException {
		StringPair datum = new StringPair();
		datum.setLeft("L");
		datum.setRight("R");

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(StringPair.class);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();

		DatumReader<StringPair> reader = new SpecificDatumReader<StringPair>(StringPair.class);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		StringPair result = reader.read(null, decoder);
		System.out.println(result.getLeft());
		System.out.println(result.getRight());
//		assertThat(result.getLeft(), is("L"));
//		assertThat(result.getRight(), is("R"));
	}
	
	@Test
	public void testDataFile() throws IOException {
		Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));

		GenericRecord datum = new GenericData.Record(schema);
		datum.put("left", "L");
		datum.put("right", "R");

		File file = new File("data.avro");
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(datum);
		dataFileWriter.close();

		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
		assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));

		assertThat(dataFileReader.hasNext(), is(true));
		GenericRecord result = dataFileReader.next();
		assertThat(result.get("left").toString(), is("L"));
		assertThat(result.get("right").toString(), is("R"));
		assertThat(dataFileReader.hasNext(), is(false));

		file.delete();
		dataFileReader.close();
		
	}

	@Test
	public void testSchemaResolution() throws IOException {
		Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
		Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPair.avsc"));

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		GenericRecord datum = new GenericData.Record(schema); 
		datum.put("left", "L");
		datum.put("right", "R");
		writer.write(datum, encoder);
		encoder.flush();

		DatumReader<GenericRecord> reader =new GenericDatumReader<GenericRecord>(schema, newSchema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		GenericRecord result = reader.read(null, decoder);
		assertThat(result.get("left").toString(), is("L"));
		assertThat(result.get("right").toString(), is("R"));
		assertThat(result.get("description").toString(), is(""));
	}

	@Test
	public void testSchemaResolutionWithDataFile() throws IOException {
		Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
		Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPair.avsc"));

		File file = new File("data.avro");

		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		dataFileWriter.create(schema, file);
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("left", "L");
		datum.put("right", "R");
		dataFileWriter.append(datum);
		dataFileWriter.close();

		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(null, newSchema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
		assertThat(schema, is(dataFileReader.getSchema())); 
		
		assertThat(dataFileReader.hasNext(), is(true));
		GenericRecord result = dataFileReader.next();
		assertThat(result.get("left").toString(), is("L"));
		assertThat(result.get("right").toString(), is("R"));
		assertThat(result.get("description").toString(), is(""));
		assertThat(dataFileReader.hasNext(), is(false));

		file.delete();
		dataFileReader.close();
	}

}
