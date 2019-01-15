package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;	
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  // Declare the boolean data type of parameter.
  private boolean myParam;
  // Use the setup() method to read the parameter in Mapper.
  public void setup(Context context) {
	  Configuration conf = context.getConfiguration();
	  myParam = conf.getBoolean("caseSensitive", true);
  }
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String line = value.toString();
	  //When the Parameter(caseSensitive) is true, output the case-sensitive answer. 	
	  if(myParam){
	  for(String word : line.split("\\W+")) {
		  if(word.length() > 0) {
			  
			  context.write(new Text(word.substring(0, 1)), new IntWritable(word.length()));
		  }
	  }
	  }
	  //When the Parameter(caseSensitive) is false, output the case-insensitive answer. 	
	  else{
		  for(String word : line.split("\\W+")) {
			  if(word.length() > 0) {
				  
				  context.write(new Text(word.substring(0, 1).toLowerCase()), new IntWritable(word.length()));
			  }
		  }
	  }
  }
}
