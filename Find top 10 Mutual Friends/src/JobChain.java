import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobChain {

	/*public static class MyPartition extends Partitioner<CustomKey,Text>{

		@Override
		public int getPartition(CustomKey key, Text value, int partition) {
			// TODO Auto-generated method stub
			int key1 = Integer.parseInt(key.id1.toString());
			int key2 = Integer.parseInt(key.id2.toString());
			return ((int)((0.5 * (key1 + key2) * (key1 + key2 + 1) + key2))) % partition;
		}	
	}
	 */	
	public static class Map extends Mapper<LongWritable,Text,Text,Text>{

		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			// Split the string
			String[] line = value.toString().split("\\s+");

			if(line.length < 3) {
				return;
			}
			//Split to get friendlist 

			String friendList[] = line[2].split(",");

			StringBuffer buffer = new StringBuffer();

			for(int i = 0;i < friendList.length;i++) {
				if(friendList[i].trim().equals("")) {
					continue;
				}
				buffer.append(friendList[i]+",");
			}

			Text mapOutputVal = new Text(buffer.toString());
			Text mapOutputKey = new Text(line[0].trim() +"\t"+line[1].trim());

			context.write(new Text("1"), new Text(mapOutputKey +"\t"+mapOutputVal));
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {


		public class QueueComparator{

			Text key;
			int size;
			public QueueComparator() {
				key = new Text();
				size = 0;
			}
			public void set(Text key,int size) {
				this.key = key;
				this.size = size;
			}
		}

		Comparator comparator = new Comparator<QueueComparator>() {

			@Override
			public int compare(QueueComparator o1, QueueComparator o2) {
				// TODO Auto-generated method stub
				return o2.size - o1.size;
			}
		};
		PriorityQueue<QueueComparator> pq = new PriorityQueue<QueueComparator>(10,comparator);
		public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException {

			StringBuffer sb = new StringBuffer();
			for(Text t: values) {
				int size = 0;

				String firstSplit[] = t.toString().split("\\s+");
				String line[] = firstSplit[2].split(",");
				for(String s: line) {
					if(s.trim().equals("")) {
						continue;
					}
					size++;
				}
				sb.append(size+"\n");
				
				QueueComparator qc = new QueueComparator();
				qc.set(new Text(firstSplit[0]+"\t"+firstSplit[1]), size);
				pq.offer(qc);
			}

			int counter = 0;
			while(counter <= 10) {
				QueueComparator qc = pq.poll();
				context.write(qc.key, new Text(qc.size+""));
				counter ++;
			}
			//context.write(key, new Text(sb.toString()));
		}

	}

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"JobChain");
		job.setJarByClass(JobChain.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setPartitionerClass(MyPartition.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);  

	}
}
