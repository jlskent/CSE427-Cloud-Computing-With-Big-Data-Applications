package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
//import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer's input are local top N from all mappers. We have a single reducer,
 * which creates the global top N.
 */
public class TopNReducer extends
		Reducer<NullWritable, Text, DoubleWritable, Text> {

	// variables
	private int N = 10; // default
	private SortedMap<Double, String> finalTopN = new TreeMap<Double, String>();
	// add lookupFile into distributed cache
	File movieTitleFile = new File("movie_titles.txt");

	// reducer input same as mapper output
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// change movieId to its name by looking up the Title file

		for (Text value : values) {
			String[] part = value.toString().split(",");
			// System.out.println("part0 is " + part[0]);//id
			// System.out.println("part1 is " + part[1]);//rating

			try {
				InputStream fis = new FileInputStream(movieTitleFile);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fis));
				for (String line = br.readLine(); line != null; line = br
						.readLine()) {
					String[] moviePart = line.split(",");
					/*
					 * moviePart[0] =id moviePart[1] = year moviePart[2] =
					 * movieTitle
					 */

					// if movieID in the list == movieId, switch id to title
					if (moviePart[0].compareTo(part[0]) == 0) {
						part[0] = moviePart[2];
						// System.out.println(moviePart[2]);

					}

					// treemap key: rating, value: title
					double rating = Double.parseDouble(part[1]);
					finalTopN.put(rating, part[0]);
				}
				br.close();
			} catch (Exception e) {
				System.err.println("Error: Target File Cannot Be Read");
			}

			// store global top N
			if (finalTopN.size() > N) {
				finalTopN.remove(finalTopN.firstKey());
			}

		}

		// write as final output of topN from treemap
		List<Double> keys = new ArrayList<Double>(finalTopN.keySet());
		for (int i = keys.size() - 1; i >= 0; i--) {
			context.write(new DoubleWritable(keys.get(i)),
					new Text(finalTopN.get(keys.get(i))));
		}

	}

	// set up function for getting size N
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10); // default is top 10
	}

}