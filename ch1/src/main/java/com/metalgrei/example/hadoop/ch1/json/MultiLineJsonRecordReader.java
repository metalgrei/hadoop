package com.metalgrei.example.hadoop.ch1.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.metalgrei.example.hadoop.ch1.json.parser.PartitionedJsonParser;

/**
 * The Class MultiLineJsonRecordReader.
 */
public class MultiLineJsonRecordReader extends RecordReader<LongWritable, Text> {
	
	/** The Constant log. */
	private static final Log LOG = LogFactory
			.getLog(MultiLineJsonRecordReader.class);
	
	/** The compression codecs. */
	private CompressionCodecFactory compressionCodecs = null;
	
	/** The start. */
	private long start;
	
	/** The pos. */
	private long pos;
	
	/** The end. */
	private long end;
	
	/** The max object length. */
	private int maxObjectLength;
	
	/** The key. */
	private LongWritable key;
	
	/** The value. */
	private Text value;
	
	/** The is. */
	private InputStream is;
	
	/** The parser. */
	private PartitionedJsonParser parser;
	
	/** The json member name. */
	private final String jsonMemberName;

	/**
	 * Instantiates a new multi line json record reader.
	 *
	 * @param jsonMemberName the json member name
	 */
	public MultiLineJsonRecordReader(String jsonMemberName) {
		this.jsonMemberName = jsonMemberName;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		StringWriter writer = new StringWriter();
		Configuration job = HadoopCompat.getConfiguration(context);
		this.maxObjectLength = job
				.getInt("mapred.multilinejsonrecordreader.maxlength",
						Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		if (codec != null) {
			is = codec.createInputStream(fileIn);
			IOUtils.copy(is, writer, "UTF-8");
			LOG.info(writer.toString());
			start = 0;
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				fileIn.seek(start);
			}
			is = fileIn;
			IOUtils.copy(is, writer, "UTF-8");
			LOG.info(writer.toString());
		}
		parser = new PartitionedJsonParser(is);
		this.pos = start;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException {
		if (pos >= end) {
			key = null;
			value = null;
			return false;
		}
		if (key == null) {
			key = new LongWritable();
		}
		if (value == null) {
			value = new Text();
		}
		while (pos < end) {
			String json = parser.nextObjectContainingMember(jsonMemberName);
			pos = start + parser.getBytesRead();
			if (json == null) {
				key = null;
				value = null;
				return false;
			}
			long jsonStart = pos - json.length();
			// if the "begin-object" position is after the end of our split,
			// we should ignore it
			//
			if (jsonStart >= end) {
				key = null;
				value = null;
				return false;
			}
			if (json.length() > maxObjectLength) {
				LOG.info("Skipped JSON object of size " + json.length()
						+ " at pos " + jsonStart);
			} else {
				key.set(jsonStart);
				value.set(json);
				return true;
			}
		}
		key = null;
		value = null;
		return false;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public Text getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split.
	 *
	 * @return the progress
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	public synchronized void close() throws IOException {
		if (is != null) {
			is.close();
		}
	}
}
