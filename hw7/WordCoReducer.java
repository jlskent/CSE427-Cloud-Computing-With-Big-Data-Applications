package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 
public class WordCoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	//input: word pair , list of counts
	//output: word pair, sum of count
  @Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
//	  System.out.println(key.toString());


//	  keep key, add up the counts
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
}