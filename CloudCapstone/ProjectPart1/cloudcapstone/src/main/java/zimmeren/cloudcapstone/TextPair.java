package zimmeren.cloudcapstone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
 
@SuppressWarnings("rawtypes")
public class TextPair implements WritableComparable {
 
    private Text first;
    private Text second;
 
    public TextPair(Text first, Text second) {
        set(first, second);
    }
 
    public TextPair() {
        set(new Text(), new Text());
    }
 
    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }
 
    public Text getFirst() {
        return first;
    }
 
    public Text getSecond() {
        return second;
    }
 
    public void set(Text first, Text second) {
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
    	TextPair tp = null;
    	if (o instanceof TextPair) {
    		tp = (TextPair) o;
    	} else {
    		return 0;
    	}
        int cmp = first.compareTo(tp.first);
 
        if (cmp != 0) {
            return cmp;
        }
 
        return second.compareTo(tp.second);
    }
 
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
    }
 
    public boolean equals(Object o)
    {
        if(o instanceof TextPair)
        {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
}
