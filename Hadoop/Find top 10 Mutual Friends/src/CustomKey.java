import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{

	Text combinedKey;
	Text id1,id2;
	ArrayList<Text> friendList;
	String string = "";
	int size;
	
	public CustomKey () {
		id1 = new Text();
		id2 = new Text();
		combinedKey = new Text();
		combinedKey.set("blah");
		friendList = new ArrayList<>();
		size = 0;
	}
	@Override
	public void readFields(DataInput in) throws IOException {

		id1.readFields(in);
		id2.readFields(in);
		combinedKey.readFields(in);
		string = in.readUTF();
		for(Text friend:friendList) {
			friend.readFields(in);
		}	
	}
	@Override
	public void write(DataOutput out) throws IOException {
		id1.write(out);
		id2.write(out);
		out.writeUTF(string);
		combinedKey.write(out);
		for(Text friend:friendList) {
			friend.write(out);
		}
	}

	@Override
	public int compareTo(CustomKey o) {
		return o.string.length() - this.string.length();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof CustomKey) {
			CustomKey other = (CustomKey)o;
			return this.id1 == other.id1 && this.id2 == other.id2;
		}
		return false;
	}
	@Override
	public int hashCode() {
		int key1 = Integer.parseInt(id1.toString());
		int key2 = Integer.parseInt(id2.toString());
		return ((int)((0.5 * (key1 + key2) * (key1 + key2 + 1) + key2)));
	}
	public Text getID() {
		return this.combinedKey;
	}
	public void setID(Text key) {
		this.combinedKey = key;
	}
	public void setID1(Text id1) {
		this.id1 = id1;
		
	}
	public void setID2(Text id2) {
		this.id2 = id2;
	}
	public void setString(String string) {
		this.string = string;
	}
	public void set(ArrayList<Text> friendList) {
		this.friendList = friendList;
		this.size = friendList.size();
	}
}
