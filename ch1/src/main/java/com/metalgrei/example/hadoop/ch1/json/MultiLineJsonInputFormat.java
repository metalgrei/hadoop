package com.metalgrei.example.hadoop.ch1.json;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;

/**
 * The Class MultiLineJsonInputFormat.
 */
public class MultiLineJsonInputFormat extends
		FileInputFormat<LongWritable, Text> {

	/** The Constant CONFIG_MEMBER_NAME. */
	public static final String CONFIG_MEMBER_NAME = "multilinejsoninputformat.member";

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		String member = HadoopCompat.getConfiguration(context).get(
				CONFIG_MEMBER_NAME);
		if (member == null) {
			throw new IOException("Missing configuration value for "
					+ CONFIG_MEMBER_NAME);
		}
		return new MultiLineJsonRecordReader(member);
	}

	/**
	 * Sets the input json member.
	 *
	 * @param job the job
	 * @param member the member
	 */
	public static void setInputJsonMember(Job job, String member) {
		HadoopCompat.getConfiguration(job).set(CONFIG_MEMBER_NAME, member);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				HadoopCompat.getConfiguration(context)).getCodec(file);
		return codec == null;
	}

}
