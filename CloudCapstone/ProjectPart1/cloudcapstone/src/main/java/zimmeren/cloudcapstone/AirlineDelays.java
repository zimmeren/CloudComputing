package zimmeren.cloudcapstone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AirlineDelays {
	public static class AirlineMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text airline = new Text();
		private FloatWritable delay = new FloatWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirlineOntimeEntry entry = new AirlineOntimeEntry();
			entry.parseEntry(value.toString());
			if (entry.valid) {
				airline.set(entry.carrier);
				delay.set(entry.arrDelay);
				context.write(airline, delay);
			}
		}
	}

	public static class AirlineSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float average = 0;
			float size = 0;
			for (FloatWritable val : values) {
				average += val.get();
				size++;
			}
			result.set(average / size);
			context.write(key, result);
		}
	}
	
	public static class SortMapper extends Mapper<Text, Text, FloatWritable, Text> {

		private final static FloatWritable amount = new FloatWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			float occurs = Float.parseFloat(value.toString());
			amount.set(occurs);
			context.write(amount, key);
		}
	}

	public static class SortReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {
		private Text result = new Text();

		public void reduce(FloatWritable key, Iterable<Text> values, Context context)
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
		job1.setMapperClass(AirlineMapper.class);
		job1.setReducerClass(AirlineSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);
		jobControl.addJob(controlledJob1);
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Sorted Airports");
		job2.setJarByClass(PopularAirports.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setOutputKeyClass(FloatWritable.class);
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