

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopSort extends Configured implements Tool {
	
	public static class TokenizeMapper extends Mapper< LongWritable, Text, Text, Text>
	{
		private Text mapKey = new Text();
		private Text mapValue = new Text();
		//			StringTokenizer st = new StringTokenizer(value.toString());
//			Text wordOut = new Text();
//			IntWritable one = new IntWritable(1);
//			while(st.hasMoreElements())

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] split = null;
			String line = value.toString();
			//assigning key as first 10 characters of line
			split = line.split("\\n"); //splitting on the basis of new line character
			for ( int i=0; i < split.length; i++)

			{//assigning key as first 10 characters of line
				mapKey.set(split[i].substring(0, 10)); 
				//			StringTokenizer st = new StringTokenizer(value.toString());
//			Text wordOut = new Text();
//			IntWritable one = new IntWritable(1);
//			while(st.hasMoreElements())
				mapValue.set(split[i].substring(10)); //assigning value as remaining characters of line
				context.write(mapKey, mapValue); //write on mapper context
			}

//			{
//				wordOut.set(st.nextToken());
//				context.write(wordOut, one);
				
//			}
			
		}
	}
	public static class SumReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce (Text term,Iterable<Text> values , Context context) throws IOException, InterruptedException
		{
			Text finalValue = null ;
			//				count++;
//				iterator.next();
//			}
//			IntWritable output = new IntWritable(count);
//			context.write(term, output);
			for(Text value : values)
			{
				finalValue = value; //assigning input to output of reducer
			}
			context.write(term, finalValue);
//			int count = 0;
//			Iterator<IntWritable> iterator = ones.iterator();
//			while(iterator.hasNext())
//			{
//				count++;
//				iterator.next();
//			}
//			IntWritable output = new IntWritable(count);
//			context.write(term, output);
		}
		
		
	}

/
	
	public int run(String[] args) throws Exception
	{
				
		//check input parameters for file paths
		if(args.length != 2)
		{
			System.err.println("Input file path or output file path missing");
			System.exit(-1);
		}
		
		//create new job
//		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		
		job.setJarByClass(HadoopSort.class);
		job.setJobName("HadoopSort");
		
		// get input and output files
		/	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//		// TODO Auto-generated method stub
//		Configuration conf= new Configuration();
//		String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
//		
//		if(otherArgs.length!=2)
//		{
//			System.err.println("Usage Sort <input_file> <output_directory>");
//			System.exit(2);
//		}
//		Job job = Job.getInstance(conf, "sortTera");
//
//		job.setJarByClass(sortTera.class);
//		job.setMapperClass(TokenizeMapper.class);
//		job.setReducerClass(SumReducer.class);
//		job.setNumReduceTasks(100);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//		boolean status = job.waitForCompletion(true);
//		if(status)
//		{
//			System.exit(0);
//		}
//		else 
//			System.exit(1);
//	}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//assign mapper and combiner class
		job.setMapperClass(TokenizeMapper.class);
		job.setCombinerClass(SumReducer.class);
		
		//assign reducer class		
		job.setReducerClass(SumReducer.class);
		//job.setNumReduceTasks(100);
		
		//define Reducers output data types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//submit job and wait for completion
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	//main method
	public static void main(String[] args) throws Exception
	{	
		//ToolRunner to run the code
		int exitCode = ToolRunner.run(new Configuration(), new HadoopSort(), args);
		System.exit(exitCode);	
	}

}
