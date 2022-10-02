import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SumMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Compile a regex pattern to find the language sub-directory parent of the text file
		Pattern pattern = Pattern.compile("([a-z]*)/(?:[\\s\\-a-zA-Z0-9]*).txt");         
		// assign input string that is the first map-reduce output
		String s = value.toString();
		// split the string on the tab delimiter into a parts array
		String[] parts = s.split("\t");
		// apply the regex pattern to the filepath element of the parts array
		Matcher matcher = pattern.matcher(parts[0]);
		// initialise a match string as an empty string
		String match = "";
		// take the letter from the second element of the parts array
		Text letter = new Text(parts[1]);
		// take the count from the third element of the parts array
		int count = Integer.parseInt(parts[2]);
		// check matcher for groups
		if (matcher.find()) {
			// assign the second group (the language) to the match string
		    match = matcher.group(1);
		}
		// assign the match string to a Text object
		Text lang = new Text(match);
		// write the language and letter as a TextPair with the count
		context.write(new TextPair(lang, letter) , new IntWritable(count));	     
	}
}