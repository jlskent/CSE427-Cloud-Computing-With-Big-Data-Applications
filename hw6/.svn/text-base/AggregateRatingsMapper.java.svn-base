package stubs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AggregateRatingsMapper extends
		Mapper<Object, Text, Text, DoubleWritable> {

	// variables
	Text K2 = new Text();
	DoubleWritable V2 = new DoubleWritable();

	// setup function
	protected void setup(Context context) throws java.io.IOException,
			java.lang.InterruptedException {

	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * handle input vale part[0]=movie id part[2]=rating part[1]will not be
		 * used
		 */
		String line = value.toString().trim();
		String[] part = line.split(",");

		/*
		 * return if format does not match
		 */
		if (part.length != 3) {
			return;
		}

		// write key-value pair movieID+rating
		K2.set(part[0]);
		V2.set(Double.parseDouble(part[2]));
		context.write(K2, V2);

	}

}