import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutualFriends {

	public  static class TextArrayWritable extends ArrayWritable {
		Text[] neighborList ;

		public Text[] getList() {
			return neighborList;
		}
		public TextArrayWritable() {
			super(Text.class);
		}

		public TextArrayWritable(String[] strings) {
			super(Text.class);
			neighborList = new Text[strings.length];
			for(int i = 0 ;i < strings.length;i++) {
				neighborList[i] = new Text(strings[i]);
			}
			set(neighborList);
		}
	}

	public static class Map extends Mapper<LongWritable,Text,Text,Text>{

		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+"); 
			int id = Integer.parseInt(line[0]); // get current id
			if(line.length < 2) {
				return;
			}
			String[] friendList = line[1].split(","); //get all friend list of current id
			
			for(int i = 0 ;i < friendList.length; i ++) {
				int neighborID = Integer.parseInt(friendList[i]);  //get a single neighbor id from the list
				String keyPair = "";
				if(neighborID > id)
					keyPair = Integer.toString(id) + "\t"+Integer.toString(neighborID);   // generate pair (currentid, neighbor id)
				else
					keyPair = Integer.toString(neighborID) + "\t"+Integer.toString(id);
				StringBuilder friends = new StringBuilder();
				
				for(int j = 0; j < friendList.length;j ++) {
					if(Integer.toString(neighborID) != friendList[j]) {
						friends.append(friendList[j]+",");
					}
				}
				context.write(new Text(keyPair), new Text(friends.toString()));
			}

			/*IntWritable p = new IntWritable(1);
				String[] strings = {"a","b","c"};
				TextArrayWritable tw = new TextArrayWritable(strings);
				context.write(p, tw);*/
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text>{

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

			HashSet<String> friendSet = new HashSet<>();
			
			String finalResult = "";
			for(Text value:values ) {
				/*String temp = value.toString();
				if(list.contains(temp)) {
					finalResult+=" " + temp;
				}else {
					list.add(temp);
				}*/
				String temp[] = value.toString().split(",");
				for(String string : temp) {
					if(!friendSet.contains(string)) {
						friendSet.add(string);
					}else {
						finalResult += string + ",";
					}
				}
				
			}
			
			context.write(key,new Text(finalResult));	
		}
	}


	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.task.io.sort.mb", "1024");
		Job job = new Job(conf, "MutualFriends");
		job.setNumReduceTasks(100);
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
