package stubs;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//input: index , line of text
//output: ({word}, {nextword}), 1
public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString().trim();
	  String[] words = line.split("\\W+");
	  
//	  loop through each line, find word-pairs to out put
	  for (int i= 0; i<words.length-1; i++){
		  if (words[i].length()>0 && words[i+1].length()>0) {
			  String twoWords = words[i].toLowerCase() + "," + words[i+1].toLowerCase();
	//		  System.out.println(twoWords);
		      context.write(new Text(twoWords), new IntWritable(1));
		  }
	  }

    
  }
}
