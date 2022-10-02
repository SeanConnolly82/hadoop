import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCombiner extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

	public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// initialise the letter count to 0
		int letterCount = 0;
		System.out.println(" In Combiner now! ");
		
		// loop over the values for each TextPair key
		for (IntWritable value : values) {
			// add each value to the letter count
			letterCount += value.get();
		}
		// write the TextPair key with the total letters integer value
		context.write(key, new IntWritable(letterCount));
	}

}