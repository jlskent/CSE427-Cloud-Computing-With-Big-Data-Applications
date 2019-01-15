package stubs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Reducer's input is: (movieId:Text, rating:DoubleWritable)
 *
 * Aggregate on the rating Could be used as combiner as well
 *
 */
public class SumReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		// for each input, produce sum of rating
		double sum = 0;
		for (DoubleWritable value : values) {
			sum += value.get();
		}
		// write output
		context.write(key, new DoubleWritable(sum));
	}
}