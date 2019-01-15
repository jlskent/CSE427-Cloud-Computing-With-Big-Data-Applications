package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends
    Mapper<LongWritable, Text, StringPairWritable, LongWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String[] names = line.split("\\W+");   //split the line up by non-word character
   
    if(names.length > 2)
    	context.write(new StringPairWritable(names[0], names[1]), new LongWritable(1));  //emit names[0] firstname, name[1] lastname
  
  }
}
