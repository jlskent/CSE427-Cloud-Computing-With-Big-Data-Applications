package stubs;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

//params: (movieId, count)
//output:
//( _ , (movieId, count)

public class TopNMapper extends Mapper<Text, Text, NullWritable, Text> {

	// variables
	private int N = 10; // default
	private TreeMap<Double, String> top = new TreeMap<Double, String>();

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		String itemId = key.toString();
		double rating = Double.parseDouble(value.toString());
		String compositeValue = itemId + "," + value.toString();
		// System.out.println(top.size());
		// System.out.println(N);

		// TreeMap key: rating, value id,rating pair
		top.put(rating, compositeValue);

		// keep only top N
		if (top.size() > N) {
			top.remove(top.firstKey());
		}

	}

	// set up function to get N
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10); // default is top
																// 10
	}

	// write results from sortedMap
	// format _, (movieId, rating)

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (String str : top.values()) {
			context.write(NullWritable.get(), new Text(str));
		}
	}

}