package zimmeren.cloudcapstone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ThreeLegTrip {
	public static class ThreeLegTripMapper extends Mapper<LongWritable, Text, TextPair, FlightWritable> {
		private TextPair existingTextPair = new TextPair();
		private TextPair searchingTextPair = new TextPair();
		private FlightWritable existingFlight = new FlightWritable();
		private FlightWritable searchingFlight = new FlightWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			AirlineOntimeEntry entry = new AirlineOntimeEntry();
			entry.parseEntry(value.toString());
			if (entry.valid) {
				//X->Y
				if (entry.depTime < 1200) {
					String date = entry.day + "/" + entry.month + "/" + entry.year;
					//because output data is so large limit to only query requested days
					//to avoid larger AWS bill
					String p1q1Date = "4/3/2008";
					String p1q2Date = "9/9/2008";
					String p1q3Date = "1/4/2008";
					String p1q4Date = "12/7/2008";
					String p1q5Date = "10/6/2008";
					String p1q6Date = "1/1/2008";
					String p2q1Date = "3/4/2008";
					String p2q2Date = "7/9/2008";
					String p2q3Date = "24/1/2008";
					String p2q4Date = "16/5/2008";
					List<String> queryDays = new ArrayList<String>();
					queryDays.add(p1q1Date);
					queryDays.add(p1q2Date);
					queryDays.add(p1q3Date);
					queryDays.add(p1q4Date);
					queryDays.add(p1q5Date);
					queryDays.add(p1q6Date);
					queryDays.add(p2q1Date);
					queryDays.add(p2q2Date);
					queryDays.add(p2q3Date);
					queryDays.add(p2q4Date);
					String p1q1Origin = "CMI";
					String p1q2Origin = "JAX";
					String p1q3Origin = "SLC";
					String p1q4Origin = "LAX";
					String p1q5Origin = "DFW";
					String p1q6Origin = "LAX";
					String p2q1Origin = "BOS";
					String p2q2Origin = "PHX";
					String p2q3Origin = "DFW";
					String p2q4Origin = "LAX";
					List<String> queryOrigins = new ArrayList<String>();
					queryOrigins.add(p1q1Origin);
					queryOrigins.add(p1q2Origin);
					queryOrigins.add(p1q3Origin);
					queryOrigins.add(p1q4Origin);
					queryOrigins.add(p1q5Origin);
					queryOrigins.add(p1q6Origin);
					queryOrigins.add(p2q1Origin);
					queryOrigins.add(p2q2Origin);
					queryOrigins.add(p2q3Origin);
					queryOrigins.add(p2q4Origin);
					String p1q1Dest = "ORD";
					String p1q2Dest = "DFW";
					String p1q3Dest = "BFL";
					String p1q4Dest = "SFO";
					String p1q5Dest = "ORD";
					String p1q6Dest = "ORD";
					String p2q1Dest = "ATL";
					String p2q2Dest = "JFK";
					String p2q3Dest = "STL";
					String p2q4Dest = "MIA";
					List<String> queryDests = new ArrayList<String>();
					queryDests.add(p1q1Dest);
					queryDests.add(p1q2Dest);
					queryDests.add(p1q3Dest);
					queryDests.add(p1q4Dest);
					queryDests.add(p1q5Dest);
					queryDests.add(p1q6Dest);
					queryDests.add(p2q1Dest);
					queryDests.add(p2q2Dest);
					queryDests.add(p2q3Dest);
					queryDests.add(p2q4Dest);
					boolean exists = false;
					for (int i = 0; i < 10; i++) {
						String day = queryDays.get(i);
						String org = queryOrigins.get(i);
						String dest = queryDests.get(i);
						if (day.equals(date) &&
							org.equals(entry.origin) &&
							dest.equals(entry.dest))
						{
							exists = true;
						}
					}
					if (!exists) {
						return;
					}
					//yes i know this will be an issue for end of month, but no queries require this
					int twoDaysAway = entry.day + 2;
					String twoDaysAwayDate = twoDaysAway + "/" + entry.month + "/" + entry.year;
					searchingTextPair.set(new Text(entry.dest), new Text(twoDaysAwayDate));
					String flight = entry.carrier + " " + entry.flightNum;
					searchingFlight.set(true, entry.arrDelay, entry.origin, entry.dest, date, 
							String.valueOf(entry.depTime), flight);
					context.write(searchingTextPair, searchingFlight);
				}
				//Y->Z
				if (entry.depTime > 1200) {
					String date = entry.day + "/" + entry.month + "/" + entry.year;
					existingTextPair.set(new Text(entry.origin), new Text(date));
					String flight = entry.carrier + " " + entry.flightNum;
					existingFlight.set(false, entry.arrDelay, entry.origin, entry.dest, date, 
							String.valueOf(entry.depTime), flight);
					context.write(existingTextPair, existingFlight);
				}
			}
		}
	}

	public static class ThreeLegTripReducer extends Reducer<TextPair, FlightWritable, Text, Text> {
		private Text resultKey = new Text();
		private Text resultValue = new Text();

		public void reduce(TextPair key, Iterable<FlightWritable> values, Context context)
				throws IOException, InterruptedException {
			List<FlightWritable> searchingList = new ArrayList<FlightWritable>();
			List<FlightWritable> existingList = new ArrayList<FlightWritable>();
			for (FlightWritable val : values) {
				FlightWritable copy = new FlightWritable();
				copy.set(val.getSearching().get(), val.getArrivalDelay().get(), 
						val.getOrigin().toString(), val.getDestination().toString(), 
						val.getDate().toString(), val.getTime().toString(), val.getFlight().toString());
				if (val.getSearching().equals(new BooleanWritable(true))) {
					searchingList.add(copy);
				} else {
					existingList.add(copy);
				}
			}
			for (FlightWritable leg1 : searchingList) {
				for (FlightWritable leg2 : existingList) {
					String flights = leg1.getOrigin().toString() + "->" +
									 leg2.getOrigin().toString() + "->" +
									 leg2.getDestination().toString();
					resultKey.set(flights + "," + leg1.getDate().toString() +":" + leg1.getTime().toString() +",");
					float totalArrDelay = leg1.getArrivalDelay().get() + leg2.getArrivalDelay().get();
					String legs = leg1.toString() + "," + leg2.toString();
					resultValue.set(totalArrDelay + "," + legs + "," + legs.hashCode());
					context.write(resultKey, resultValue);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ThreeLegTrip");
	    job.setJarByClass(ThreeLegTrip.class);
	    job.setMapperClass(ThreeLegTripMapper.class);
	    job.setMapOutputValueClass(FlightWritable.class);
	    job.setMapOutputKeyClass(TextPair.class);
	    job.setReducerClass(ThreeLegTripReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

