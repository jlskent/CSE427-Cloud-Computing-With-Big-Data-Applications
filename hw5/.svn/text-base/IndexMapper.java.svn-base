package stubs;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


//input
public class IndexMapper extends Mapper<Text, Text, Text, Text> {
	
//	setup function, take file name
	  String fileName = new String();
	  protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException {
		     fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
//		     System.out.println(fileName);
	  }
	

//	  map function
//	  inputKey: a key-value pair (index, line)
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	
	    String line = value.toString();
//  	System.out.println(key);
//  	System.out.println("---------------------");
//  	System.out.println(value);	   
	    
//    	String indexOfLine = "";    	    	
	    for (String word : line.split("\\W+")) {
	        if (word.length() > 0) {
		        String result;
//		        match number notes for self reference
//		        Pattern p = Pattern.compile("(\\d+)");
//		        Matcher m = p.matcher(word);
//		        if (m.find()){
//		        	indexOfLine = m.group(0);
//		            System.out.println(m.group(0)); // whole matched expression
//		        	continue;
//		        }

	            result = fileName + "@" + String.valueOf(key);
//	            output:
//	            format as follows word+filename+@+#line, for MapperOutput
// 	            eg. honeysuckle 2kinghenryiv@1038,midsummernightsdream@2175

	            context.write(new Text(word.toLowerCase()), new Text(result));
	        }
	  
	    }
	  
	  
	  
  }

	  

    
  
}