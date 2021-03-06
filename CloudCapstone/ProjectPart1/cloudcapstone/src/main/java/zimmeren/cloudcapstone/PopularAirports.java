package zimmeren.cloudcapstone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopularAirports {
	public static class AirportMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text origin = new Text();
		private Text dest = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirlineOntimeEntry entry = new AirlineOntimeEntry();
			entry.parseEntry(value.toString());
			if (entry.valid) {
				origin.set(entry.origin);
				context.write(origin, one);
				dest.set(entry.dest);
				context.write(dest, one);
			}
		}
	}

	public static class AirportSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class PopularMapper extends Mapper<Text, Text, IntWritable, Text> {

		private final static IntWritable amount = new IntWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int occurs = Integer.parseInt(value.toString());
			amount.set(occurs);
			context.write(amount, key);
		}
	}

	public static class PopularReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val);
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobControl jobControl = new JobControl("jobChain");
		
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "Popular Airports");
		job1.setJarByClass(PopularAirports.class);
		job1.setMapperClass(AirportMapper.class);
		job1.setCombinerClass(AirportSumReducer.class);
		job1.setReducerClass(AirportSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);
		jobControl.addJob(controlledJob1);
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Sorted Airports");
		job2.setJarByClass(PopularAirports.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapperClass(PopularMapper.class);
		job2.setCombinerClass(PopularReducer.class);
		job2.setReducerClass(PopularReducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);
		controlledJob2.addDependingJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		
		while (!jobControl.allFinished()) {
			try { 
				Thread.sleep(1000);
			} catch(Exception e) {
				
			}
		}
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}