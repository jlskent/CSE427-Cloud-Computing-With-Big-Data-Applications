package stubs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*; 

import javax.naming.Context;



//import javax.security.auth.login.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

//variables
  private Configuration configuration;
//  hashset to store paritioner file content
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();
    
  
/*  configuration
 * similar to setup func in Mapper
  read the files line by line and store them in hashsets*/
  
  @Override
  public void setConf(Configuration configuration) {
	 Configuration conf = configuration;
//   File positiveFile = new File("/positive-words.txt");
//   File negativeFile = new File("/positive-words.txt");
//	 conf.get(PARTITIONER_PATH, DEFAULT_PATH);

	 
//	 scanners
     Scanner positiveFile;
     Scanner negativeFile;

     
//   hadle error
//   skip lines that starts with ";", and close scanner after use
	try {
//		read file from path in distributed cache
		positiveFile = new Scanner(new File("positive-words.txt"));
	     while (positiveFile.hasNext()) {
//	    	 get line from content, and skip line with ";" start
	    	   String nextLine = positiveFile.nextLine();
	    	   if (nextLine.startsWith(";")) {
	    		   continue;
	    	   }
//	    	   add it to hashset
	    	 positive.add(positiveFile.next());
	     }
	     positiveFile.close();
	} catch (FileNotFoundException e) {
		System.out.println("read file error");
		e.printStackTrace();
	}

	
	try {
		negativeFile = new Scanner(new File("negative-words.txt"));
	     while (negativeFile.hasNext()) {
	    	   String nextLine = negativeFile.nextLine();
	    	   if (nextLine.startsWith(";")) {
	    		   continue;
	    	   }
	    	 negative.add(negativeFile.next());
	     }
	     negativeFile.close();
	} catch (FileNotFoundException e) {
		System.out.println("read file error");
		e.printStackTrace();
	}
     

     
  }
  

//configuration
  @Override
  public Configuration getConf() {
    return configuration;
  }


  
/*Param:
	input data from mapper output, and number of ReduceTasks set it driver as 3
	output number of the Reducer*/
  
  
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	  
//	  send to 0 for positive words
//	  send to 1 for negative words
//	  send to 2 for neutral
//	  note: if a word is in both list, consider positive
	  
	  if (positive.contains(key.toString())){
		  return 0;
	  }
	  
	  else if (negative.contains(key.toString())){
		  return 1;
	  }
	  
	  else {
	  return 2;
	  }
	  
  }
}
