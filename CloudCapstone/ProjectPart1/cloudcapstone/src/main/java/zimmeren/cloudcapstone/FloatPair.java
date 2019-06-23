package zimmeren.cloudcapstone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
 
@SuppressWarnings("rawtypes")
public class FloatPair implements WritableComparable {
 
    private FloatWritable first;
    private FloatWritable second;
 
    public FloatPair(FloatWritable first, FloatWritable second) {
        set(first, second);
    }
 
    public FloatPair() {
        set(new FloatWritable(), new FloatWritable());
    }
 
    public FloatPair(float first, float second) {
        set(new FloatWritable(first), new FloatWritable(second));
    }
 
    public FloatWritable getFirst() {
        return first;
    }
 
    public FloatWritable getSecond() {
        return second;
    }
 
    public void set(FloatWritable first, FloatWritable second) {
        this.first = first;
        this.second = second;
    }
 
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
 
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
 
    public String toString() {
        return first + " " + second;
    }
 
    public int compareTo(Object o) {
    	FloatPair fp = null;
    	if (o instanceof FloatPair) {
    		fp = (FloatPair) o;
    	} else {
    		return 0;
    	}
        int cmp = first.compareTo(fp.first);
 
        if (cmp != 0) {
            return cmp;
        }
 
        return second.compareTo(fp.second);
    }
 
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
    }
 
    public boolean equals(Object o)
    {
        if(o instanceof FloatPair)
        {
            FloatPair fp = (FloatPair) o;
            return first.equals(fp.first) && second.equals(fp.second);
        }
        return false;
    }
}
