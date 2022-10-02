import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TotalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// assign input string that is the second map-reduce output
		String s = value.toString();
		// split the string on the tab delimiter into a parts array
		String[] parts = s.split("\t");
		// take the language from the first element of the parts array
		Text lang = new Text(parts[0]);
		// take the count from the third element of the parts array
		int count = Integer.parseInt(parts[2]);		
		// write the language as Text object with the count
		context.write(new Text(lang), new IntWritable(count));	
	}
}