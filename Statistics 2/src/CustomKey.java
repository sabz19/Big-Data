import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey> {

	private int key1,key2;
	
	public CustomKey() {
		key1 = 1;
		
	}
	@Override
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		key1 = in.readInt();
		key2 = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(key1);
		out.writeInt(key2);
	}
	public void setValues(int key1,int key2) {
		this.key1 = 1;
		this.key2 = key2;
	}
	
	public int getKey1() {
		return this.key1;
	}
	public int getKey2() {
		return this.key2;
	}

	@Override
	public int compareTo(CustomKey o) {
		if(this.key2 < o.key2) {
			return -1;
		}
		if(this.key2 > o.key2) {
			return 1;
		}
		return 0;
		
		
	}
	@Override 
	public boolean equals(Object o) {
		if(o instanceof CustomKey) {
			CustomKey other = (CustomKey)o;
			return this.key1 == other.key1;
		}
		return false;
		
	}
	@Override 
	public int hashCode() {
		return key1;
	}

}
