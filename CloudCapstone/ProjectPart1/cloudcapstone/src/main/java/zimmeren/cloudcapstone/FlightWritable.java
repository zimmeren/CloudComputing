package zimmeren.cloudcapstone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class FlightWritable implements Writable {
	
	private BooleanWritable searching;
    private FloatWritable arrivalDelay;
    private Text origin;
    private Text destination;
    private Text date;
    private Text time;
    private Text flight;
    
    public FlightWritable() {
    	set(new BooleanWritable(), new FloatWritable(),
        		new Text(), new Text(), new Text(),
        		new Text(), new Text());
    }
 
    public FlightWritable(boolean searching, float arrivalDelay, String origin,
    		String destination, String date, String time, String flight) {
        set(new BooleanWritable(searching), new FloatWritable(arrivalDelay),
        		new Text(origin), new Text(destination), new Text(date),
        		new Text(time), new Text(flight));
    }
    
    public BooleanWritable getSearching() {
    	return searching;
    }
    
    public FloatWritable getArrivalDelay() {
    	return arrivalDelay;
    }
    
    public Text getOrigin() {
    	return origin;
    }
    
    public Text getDestination() {
    	return destination;
    }
    
    public Text getDate() {
    	return date;
    }
    
    public Text getTime() {
    	return time;
    }
    
    public Text getFlight() {
    	return flight;
    }
 
    public void set(BooleanWritable searching, FloatWritable arrivalDelay, Text origin, 
    		Text destination, Text date, Text time, Text flight) {
        this.searching = searching;
        this.arrivalDelay = arrivalDelay;
        this.origin = origin;
        this.destination = destination;
        this.date = date;
        this.time = time;
        this.flight = flight;
    }
    
    public void set(boolean searching, float arrivalDelay, String origin,
    		String destination, String date, String time, String flight) {
        set(new BooleanWritable(searching), new FloatWritable(arrivalDelay),
        		new Text(origin), new Text(destination), new Text(date),
        		new Text(time), new Text(flight));
    }

	public void readFields(DataInput in) throws IOException {
		searching.readFields(in);
		arrivalDelay.readFields(in);
		origin.readFields(in);
		destination.readFields(in);
		date.readFields(in);
		time.readFields(in);
		flight.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		searching.write(out);
		arrivalDelay.write(out);
		origin.write(out);
		destination.write(out);
		date.write(out);
		time.write(out);
		flight.write(out);
	}

	public String toString() {
		return searching.toString() + ", " +
				arrivalDelay.toString() + ", " +
				origin.toString() + ", " +
				destination.toString() + ", " +
				date.toString() + ", " +
				time.toString() + ", " +
				flight.toString();
	}
}
