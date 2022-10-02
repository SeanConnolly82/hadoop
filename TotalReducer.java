import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		// initialise the letter count to 0
		int totalLetterCount = 0;

		System.out.println(" In Reducer now! ");
		
		// loop over the values for each TextPair key
		for (IntWritable value : values) {
			// add each value to the letter count
			totalLetterCount += value.get();
		}
		// write the TextPair key with the total letters integer value
		context.write(key, new IntWritable(totalLetterCount));
	}

}