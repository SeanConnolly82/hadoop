
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetterCount {
	public static void main(String[] args) throws Exception {
		
		// check to confirm the right number of command line arguments
		if (args.length != 4) {
			System.err.println("Usage: Letter Frequency <input path> <temp path job 1> <temp path job 2>  <output path>");
			System.exit(-1); }

		System.out.println(" In Driver now!");
		
		// create a Job instance for the first map reduce job
		Job job1 = Job.getInstance();
		job1.setJarByClass(LetterCount.class);
		// assign a name to the job
		job1.setJobName("LetterCount");
		
		// assign the mapper class to job1
		job1.setMapperClass(CountMapper.class);
		// assign the combiner class to job1
		job1.setCombinerClass(CountCombiner.class);
		// assign the reducer class to job1
		job1.setReducerClass(CountReducer.class);

		// set the output key and value for the map reduce process
		job1.setOutputKeyClass(TextPair.class);
		job1.setOutputValueClass(IntWritable.class);
		
		// enable recursive processing of files in sub-directories
		FileInputFormat.setInputDirRecursive(job1, true);
		// use the command line arguments to set the input and output paths
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		// wait for first job to complete before moving to the second
		job1.waitForCompletion(true);
		
		// create a Job instance for the second map reduce job
		Job job2 = Job.getInstance();
		job2.setJarByClass(LetterCount.class);
		job2.setJobName("LanguageSum");

		job2.setMapperClass(SumMapper.class);
		// use the reducer class as the combiner class
		job2.setCombinerClass(SumReducer.class);
		job2.setReducerClass(SumReducer.class);

		job2.setOutputKeyClass(TextPair.class);
		job2.setOutputValueClass(IntWritable.class);
		// the input of the second map reduce process is the output of the first map reduce process
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		// assign the result of the job processing to a variable
		
		// wait for second job to complete before moving to the third
		job2.waitForCompletion(true);
		// create a Job instance for the second map reduce job
		Job job3 = Job.getInstance();
		job3.setJarByClass(LetterCount.class);
		job3.setJobName("LanguageTotal");

		job3.setMapperClass(TotalMapper.class);
		job3.setCombinerClass(TotalReducer.class);
		job3.setReducerClass(TotalReducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		// the input of the third map reduce process is the output of the second map reduce process
		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		// assign the result of the job processing to a variable
		int code = job3.waitForCompletion(true) ? 0 : 1;
		// get the LetterCounter from the CountMapper class
		Counter mapperCounter = job1.getCounters().findCounter(CountMapper.LetterCounter.MAP_LETTER_COUNTER);
		// print the number of letters processed to the console
		System.out.println("Total number of letters processed in MAP: " +mapperCounter.getValue());
		// exit with success (0) or error (1)
		System.exit(code);
	}
}