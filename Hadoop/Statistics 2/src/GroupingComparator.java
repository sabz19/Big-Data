import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
	protected GroupingComparator() {
			super(CustomKey.class,true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CustomKey k1 = (CustomKey)w1;
			CustomKey k2 = (CustomKey)w2;
			return k1.getKey1() - k2.getKey1();
		}
	}

