package stubs;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*Job chaining
 * we configure job 1 to aggregate local top
 * then configure job 2 to create final result
 * 
*/


public class TopNDriver  extends Configured implements Tool {
//	specify intermediate data path which is job1 output and job2 input
   private static final String OUTPUT_PATH = "job1_output";

   public int run(String[] args) throws Exception {
	   
//test args
	  System.out.println(args[0]);//value N
	  System.out.println(args[1]);//input Dir
	  System.out.println(args[2]);//output Dir

      /*
       * Job 1
       */
      Configuration conf1 = getConf();
      Job job1 = Job.getInstance(conf1);
      job1.setJobName("AggreagateRatings");
      job1.setJarByClass(TopNDriver.class);
      job1.setMapperClass(AggregateRatingsMapper.class);
      job1.setReducerClass(SumReducer.class);
      job1.setInputFormatClass(TextInputFormat.class);
	  job1.setMapOutputKeyClass(Text.class);
	  job1.setMapOutputValueClass(DoubleWritable.class);
      TextInputFormat.addInputPath(job1, new Path(args[1]));
      TextOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));
      job1.setNumReduceTasks(1);
      
//set combiner if necessary
//      job1.setCombinerClass(SumReducer.class); 
//    wait for job1 to complete
      job1.waitForCompletion(true);
      System.out.println("job1 complete");

      
/*      job2
*/      
      Job job = Job.getInstance(getConf());
//      get N from input
      int N = Integer.parseInt(args[0]);
//      int N = 10;
      job.getConfiguration().setInt("N", N);
      job.setJobName("TopNDriver");
      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setJarByClass(TopNDriver.class);
      job.setMapperClass(TopNMapper.class);
      job.setReducerClass(TopNReducer.class);
      job.setNumReduceTasks(1);
      job.setMapOutputKeyClass(NullWritable.class);   
      job.setMapOutputValueClass(Text.class);   
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);

       // args[1] = input directory
       // args[2] = output directory
      FileInputFormat.setInputPaths(job, new Path(OUTPUT_PATH));
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      boolean status = job.waitForCompletion(true);
      return status ? 0 : 1;
   }

   /**
   * The main driver for "Top N" program.
   * 
   */
   public static void main(String[] args) throws Exception {
	   
		int exitCode = ToolRunner.run(new Configuration(),
				new TopNDriver(), args);
		System.exit(exitCode);
		
	   
	   
   }

}