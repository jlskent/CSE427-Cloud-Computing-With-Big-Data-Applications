package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
//for local test only
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//
//Driver
//job 1 aggregate the rating
//Note: conf not valid if we specified conf in job2 with chaining job execution

public class AggregateRatings extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new AggregateRatings(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out
					.printf("Usage: AvgWordLength <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = new Job(getConf());

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(AggregateRatings.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("AggreateRatings");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(AggregateRatingsMapper.class);
		job.setReducerClass(SumReducer.class);
		
//		note: with the format we put everything now in the Mapper input value
        job.setInputFormatClass(TextInputFormat.class);

		/*
		 * Specify the job's output key and value classes.
		 */

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
        job.setNumReduceTasks(1);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		// System.exit(success ? 0 : 1);
		return success ? 0 : 1;

	}

}
