import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Stats4 {

	public static class MyPartition extends Partitioner<CustomKey,Text>{

		@Override
		public int getPartition(CustomKey key, Text value, int partition) {
			return key.getKey1() % partition;
		}	
	}
	
	public static class Map extends Mapper<LongWritable,Text,CustomKey,IntWritable>{
	
		CustomKey ck = new CustomKey();
		IntWritable iw = new IntWritable();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if(line.trim().equals("")) {
				return;
			}
			ck.setValues(1, Integer.parseInt(line));
			iw.set(Integer.parseInt(value.toString()));
			context.write(ck, iw);
		}
	}
	
	public static class Reduce extends Reducer<CustomKey,IntWritable,Text,Text>{
		Text result = new Text();
		
		public void reduce(CustomKey ck,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException {
			
			Iterator<IntWritable> it = values.iterator();
			int counter = 0;
			ArrayList<Integer> list = new ArrayList<>();
			
			StringBuffer sb = new StringBuffer();
			for(IntWritable iw : values) {
				int x = iw.get();
				sb.append(Integer.toString(x));
				list.add(x);
				counter++;
			}
			
			int median = 0;
			int num1 = list.get(counter/2);
			int num2 = list.get((counter/2) + 1);
			if(counter % 2 == 0) {
				median = (num1 + num2)/2;
			}else {
				median = num1;
			}
			int min = list.get(0);
			int max = list.get(counter -1);
			result.set(min + "\t"+ max + "\t"+ median);
			context.write(new Text(""), result);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	  	// get all args
	  	
	  	// create a job with name "wordcount"
	  	Job job = new Job(conf, "Stat4");
	  	job.setJarByClass(Stats4.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setPartitionerClass(MyPartition.class);
	  	job.setGroupingComparatorClass(GroupingComparator.class);
	  	job.setMapOutputKeyClass(CustomKey.class);
	  	job.setMapOutputValueClass(IntWritable.class);
	  	job.setOutputKeyClass(Text.class);
	  	job.setOutputValueClass(Text.class);
	  	
	  	FileInputFormat.addInputPath(job, new Path(args[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  	//Wait till job completion
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
