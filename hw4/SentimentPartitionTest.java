package stubs;

import static org.junit.Assert.assertEquals;

import java.util.*; 
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner spart;

	@Test
	public void testSentimentPartition() {

		spart=new SentimentPartitioner();
//		spart=new SentimentPartitioner<Text, IntWritable>();

		
		spart.setConf(new Configuration());
		
//		variables to test four words
		int result;		
		int resultA;		
		int resultB;		
		int resultC;		

		
		/*
		 * A test for word "beauty" with expected outcome 0 would   
		 * look like this:
		 */
		result = spart.getPartition(new Text("beauty"), new IntWritable(23), 3);
		assertEquals(result,0);	

		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
		
		
//      use positive word "love" to test getPartition() result, it should be sent to Reducer 0
		resultA = spart.getPartition(new Text("love"), new IntWritable(23), 3);
		assertEquals(resultA,0);	
		
//      use positive word "deadly" to test getPartition() result, it should be sent to Reducer 1
		resultB = spart.getPartition(new Text("deadly"), new IntWritable(23), 3);
		assertEquals(resultB,1);	
		
//      use positive word "zodiac" to test getPartition() result, it should be sent to Reducer 2
		resultC = spart.getPartition(new Text("zodiac"), new IntWritable(23), 3);
		assertEquals(resultC,2);	
		
		
		
	}

}
