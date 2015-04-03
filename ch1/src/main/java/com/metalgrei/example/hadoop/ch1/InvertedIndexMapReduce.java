package com.metalgrei.example.hadoop.ch1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * The Class InvertedIndexMapReduce.
 */
final class InvertedIndexMapReduce {
  
  /**
   * The main method.
   *
   * @param args the arguments
   * @throws Exception the exception
   */
  public static void main(String... args) throws Exception {

    runJob(
        Arrays.copyOfRange(args, 0, args.length - 1),
        args[args.length - 1]);
  }

  /**
   * Run job.
   *
   * @param input the input
   * @param output the output
   * @throws Exception the exception
   */
  public static void runJob(String[] input, String output)
      throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(InvertedIndexMapReduce.class);
    job.setMapperClass(InvertedIndexMap.class);
    job.setReducerClass(InvertedIndexReduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    Path outputPath = new Path(output);

    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    job.waitForCompletion(true);
  }

  /**
   * The Class Map.
   */
  public static class InvertedIndexMap
      extends Mapper<LongWritable, Text, Text, Text> {

    /** The document id. */
    private Text documentId;
    
    /** The word. */
    private Text word = new Text();

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) {
      String filename =
          ((FileSplit) context.getInputSplit()).getPath().getName();
      documentId = new Text(filename);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      for (String token : StringUtils.split(value.toString())) {
        word.set(token);
        context.write(word, documentId);
      }
    }
  }

  /**
   * The Class Reduce.
   */
  public static class InvertedIndexReduce
      extends Reducer<Text, Text, Text, Text> {

    /** The doc ids. */
    private Text docIds = new Text();
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
        throws IOException, InterruptedException {

      HashSet<Text> uniqueDocIds = new HashSet<Text>();
      for (Text docId : values) {
        uniqueDocIds.add(new Text(docId));
      }
      docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
      context.write(key, docIds);
    }
  }
}
