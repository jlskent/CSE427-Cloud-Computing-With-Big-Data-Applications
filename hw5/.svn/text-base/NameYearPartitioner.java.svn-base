package example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NameYearPartitioner<K2, V2> extends
		HashPartitioner<StringPairWritable, Text> {

	/**
	 * Partition Name/Year pairs according to the first string (last name) in the string pair so 
	 * that all keys with the same last name go to the same reducer, even if  second part
	 * of the key (birth year) is different.
	 */
	//set Reducer number to 1 or
	//specify partitioner to have global sort when we have 2 partitioners
	public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {
		if (key.getLeft().substring(0,1).compareTo("M") > 0){
			return 1;
		}
		else {
			return 0;
		}
		
//		return (key.getLeft().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	
	}
}
