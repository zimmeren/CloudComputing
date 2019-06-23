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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirportAirportMeanDelay {
	public static class AirportAirportMeanMapper extends Mapper<LongWritable, Text, TextPair, FloatPair> {
		private final static FloatWritable one = new FloatWritable(1);
		private TextPair airportAirline = new TextPair();
		private FloatPair delay = new FloatPair();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirlineOntimeEntry entry = new AirlineOntimeEntry();
			entry.parseEntry(value.toString());
			if (entry.valid) {
				airportAirline.set(new Text(entry.origin), new Text(entry.dest));
				delay.set(new FloatWritable(entry.arrDelay), one);
				context.write(airportAirline, delay);
			}
		}
	}
	
	public static class AirportAirportMeanCombiner extends Reducer<TextPair, FloatPair, TextPair, FloatPair> {
		private FloatPair result = new FloatPair();

		public void reduce(TextPair key, Iterable<FloatPair> values, Context context)
				throws IOException, InterruptedException {
			float average = 0;
			float size = 0;
			for (FloatPair val : values) {
				average += val.getFirst().get();
				size += val.getSecond().get();
			}
			result.set(new FloatWritable(average), new FloatWritable(size));
			context.write(key, result);
		}
	}

	public static class AirportAirportMeanReducer extends Reducer<TextPair, FloatPair, TextPair, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(TextPair key, Iterable<FloatPair> values, Context context)
				throws IOException, InterruptedException {
			float average = 0;
			float size = 0;
			for (FloatPair val : values) {
				average += val.getFirst().get();
				size += val.getSecond().get();
			}
			result.set(average / size);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "AirportAirportDelays");
	    job.setJarByClass(AirportAirlineDelays.class);
	    job.setMapperClass(AirportAirportMeanMapper.class);
	    job.setMapOutputValueClass(FloatPair.class);
	    job.setCombinerClass(AirportAirportMeanCombiner.class);
	    job.setReducerClass(AirportAirportMeanReducer.class);
	    job.setOutputKeyClass(TextPair.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
