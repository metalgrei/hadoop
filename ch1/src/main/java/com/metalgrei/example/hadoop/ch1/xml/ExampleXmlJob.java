package com.metalgrei.example.hadoop.ch1.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The Class ExampleXmlJob.
 */
public class ExampleXmlJob {
	
	private static final Log LOG = LogFactory
			.getLog(ExampleXmlJob.class);

	/**
	 * The Class XmlInputFormat1.
	 */
	public static class XmlInputFormat1 extends TextInputFormat {

		/** The Constant START_TAG_KEY. */
		public static final String START_TAG_KEY = "xmlinput.start";

		/** The Constant END_TAG_KEY. */
		public static final String END_TAG_KEY = "xmlinput.end";

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.lib.input.TextInputFormat#createRecordReader
		 * (org.apache.hadoop.mapreduce.InputSplit,
		 * org.apache.hadoop.mapreduce.TaskAttemptContext)
		 */
		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context) {
			return new XmlRecordReader();
		}

		/**
		 * XMLRecordReader class to read through a given xml document to output
		 * xml blocks as records as specified by the start tag and end tag.
		 */

		public static class XmlRecordReader extends
				RecordReader<LongWritable, Text> {

			/** The start tag. */
			private byte[] startTag;

			/** The end tag. */
			private byte[] endTag;

			/** The start. */
			private long start;

			/** The end. */
			private long end;

			/** The fsin. */
			private FSDataInputStream fsin;

			/** The buffer. */
			private DataOutputBuffer buffer = new DataOutputBuffer();

			/** The key. */
			private LongWritable key = new LongWritable();

			/** The value. */
			private Text value = new Text();

			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache
			 * .hadoop.mapreduce.InputSplit,
			 * org.apache.hadoop.mapreduce.TaskAttemptContext)
			 */
			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
				endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
				FileSplit fileSplit = (FileSplit) split;

				// open the file and seek to the start of the split
				start = fileSplit.getStart();
				end = start + fileSplit.getLength();
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				fsin = fs.open(fileSplit.getPath());
				fsin.seek(start);

			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
			 */
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (fsin.getPos() < end) {
					if (readUntilMatch(startTag, false)) {
						try {
							buffer.write(startTag);
							if (readUntilMatch(endTag, true)) {
								key.set(fsin.getPos());
								value.set(buffer.getData(), 0,
										buffer.getLength());
								return true;
							}
						} finally {
							buffer.reset();
						}
					}
				}
				return false;
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
			 */
			@Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				return key;
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
			 */
			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return value;
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see org.apache.hadoop.mapreduce.RecordReader#close()
			 */
			@Override
			public void close() throws IOException {
				fsin.close();
			}

			/*
			 * (non-Javadoc)
			 * 
			 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
			 */
			@Override
			public float getProgress() throws IOException {
				return (fsin.getPos() - start) / (float) (end - start);
			}

			/**
			 * Read until match.
			 *
			 * @param match
			 *            the match
			 * @param withinBlock
			 *            the within block
			 * @return true, if successful
			 * @throws IOException
			 *             Signals that an I/O exception has occurred.
			 */
			private boolean readUntilMatch(byte[] match, boolean withinBlock)
					throws IOException {
				int i = 0;
				while (true) {
					int b = fsin.read();
					// end of file:
					if (b == -1)
						return false;
					// save to buffer:
					if (withinBlock)
						buffer.write(b);
					// check if we're matching:
					if (b == match[i]) {
						i++;
						if (i >= match.length)
							return true;
					} else
						i = 0;
					// see if we've passed the stop point:
					if (!withinBlock && i == 0 && fsin.getPos() >= end)
						return false;
				}
			}
		}
	}

	/**
	 * The Class Map.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String document = value.toString();
			LOG.info("‘" + document + "‘");
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(
								new ByteArrayInputStream(document.getBytes()));
				String propertyName = "";
				String propertyValue = "";
				String currentElement = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
					case XMLStreamConstants.START_ELEMENT: // START_ELEMENT:
						currentElement = reader.getLocalName();
						break;
					case XMLStreamConstants.CHARACTERS: // CHARACTERS:
						if (currentElement.equalsIgnoreCase("name")) {
							propertyName += reader.getText();
							LOG.info("propertName" + propertyName);
						} else if (currentElement.equalsIgnoreCase("value")) {
							propertyValue += reader.getText();
							LOG.info("propertyValue" + propertyValue);
						}
						break;
					}
				}
				reader.close();
				context.write(new Text(propertyName.trim()), new Text(
						propertyValue.trim()));

			} catch (Exception e) {
				throw new IOException(e);

			}

		}
	}

	/**
	 * The Class Reduce.
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
		 * .Reducer.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text("<xml>"), null);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce
		 * .Reducer.Context)
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text("</xml>"), null);
		}

		/** The output key. */
		private Text outputKey = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				outputKey.set(constructPropertyXml(key, value));

				context.write(outputKey, null);
			}
		}

		/**
		 * Construct property xml.
		 *
		 * @param name
		 *            the name
		 * @param value
		 *            the value
		 * @return the string
		 */
		public static String constructPropertyXml(Text name, Text value) {
			StringBuilder sb = new StringBuilder();
			sb.append("<property><name>").append(name).append("</name><value>")
					.append(value).append("</value></property>");
			return sb.toString();
		}
	}

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path outputPath = new Path(args[1]);

		conf.set("xmlinput.start", "<match>");
		conf.set("xmlinput.end", "</match>");

		Job job = new Job(conf);
		job.setJarByClass(ExampleXmlJob.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ExampleXmlJob.Map.class);
		job.setReducerClass(ExampleXmlJob.Reduce.class);

		job.setInputFormatClass(XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
	}

}
