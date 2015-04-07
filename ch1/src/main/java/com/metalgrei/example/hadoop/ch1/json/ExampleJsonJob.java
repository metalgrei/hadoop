package com.metalgrei.example.hadoop.ch1.json;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Class ExampleJob.
 */
final class ExampleJsonJob extends Configured implements Tool {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(final String[] args) throws Exception {
		int rest = ToolRunner.run(new Configuration(), new ExampleJsonJob(), args);
		System.exit(rest);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(final String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		Configuration conf = super.getConf();
		Job job = new Job(conf);
		job.setJarByClass(ExampleJsonJob.class);
		job.setMapperClass(ExampleJobMap.class);
		job.setNumReduceTasks(0);
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);
		// use the JSON input format
		job.setInputFormatClass(MultiLineJsonInputFormat.class);
		// specify the JSON attribute name which is used to determine which
		// JSON elements are supplied to the mapper
		MultiLineJsonInputFormat.setInputJsonMember(job, "colorName");
		if (job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	/**
	 * The Class ExampleJobMap.
	 */
	public static class ExampleJobMap extends Mapper<LongWritable, Text, Text, Text> {
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// strip-out newlines
			String formatted = value.toString().replaceAll("\n", " ");
			// emit the tuple and the original contents of the line
			context.write(new Text(String.format("Got JSON: '%s'", formatted)),
					null);
		}
	}

}
