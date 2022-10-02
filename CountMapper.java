import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CountMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
	
	// path variable to keep track of filepaths
	public Path filesplit;
	
	// set up counter to count the number of letters mapped
	static enum LetterCounter {
		MAP_LETTER_COUNTER
	}
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
 
		// keep track of filepaths of files processed, used in second map for language key
		filesplit = ((FileSplit)context.getInputSplit()).getPath();
		// assign input string of the book text to a string variable
		String s = value.toString();
		// split string into chunks of words, splitting on anything that isn't a letter
		String wordChunks[] = s.split("[^\\p{L}]+");
		// loop over the word chunks
        for (String wordChunk : wordChunks) {
        	// split the word chunk into each letter
		    for (String letter : wordChunk.split("")) {
		    	// some empty letters were still getting through
		    	if (letter.isEmpty()) {
	        		continue;
	        	}
		    	// write the filepath and lowercase letter to a TextPair object
		    	context.write(new TextPair(filesplit.toString(), letter.toLowerCase()), new IntWritable(1));
		    	// increment the letter counter every loop iteration
		    	context.getCounter(LetterCounter.MAP_LETTER_COUNTER).increment(1);
		    }
		}
	}
}