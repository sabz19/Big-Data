import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StatCompute {
	
	public static class MyCombiner extends Reducer < Text,Text,Text,Text>{
		Text mapperSums = new Text();
		
		public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException {
			double sum = 0,squareSum = 0;
			int total_Count = 0;
			for(Text value: values) {
				int num = Integer.parseInt(value.toString());
				sum += num;
				squareSum += Math.pow(num, 2);
				total_Count ++;
			}
			mapperSums.set(new Text(Double.toString(sum)+" "+ Double.toString(squareSum) + " "+ 
			Integer.toString(total_Count)));
			context.write(key, mapperSums);
		}
	}
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			context.write(new Text("1"),value);
		}
	}

	public static class Reduce extends Reducer<Text,Text,DoubleWritable,DoubleWritable> {
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			//Compute mean
			double totalSum = 0,totalSumSquares = 0,variance = 0;
			int totalCount = 0;
			
			for(Text value:values) {
				String[] s = value.toString().split("\\s+");
				double sum = Double.parseDouble(s[0]);
				double squareSum = Double.parseDouble(s[1]);
				int count = Integer.parseInt(s[2]);
				//cache.add(num);
				totalSum += sum;
				totalSumSquares += squareSum;
				totalCount += count;
				
			}
			
			double mean = (double)totalSum/totalCount;
			variance = (double)((totalSumSquares/totalCount) - Math.pow(mean, 2));
			context.write(new DoubleWritable(mean),new DoubleWritable(variance));
		
		}
	}

  	// Driver program
  	public static void main(String[] args) throws Exception {
  	Configuration conf = new Configuration();
  	// get all args
  	
  	// create a job with name "wordcount"
  	Job job = new Job(conf, "Stats");
  	job.setJarByClass(StatCompute.class);
  	job.setMapperClass(Map.class);
  	job.setReducerClass(Reduce.class);
  	job.setMapOutputKeyClass(Text.class);
  	job.setMapOutputValueClass(Text.class);
  	job.setCombinerClass(MyCombiner.class);
  	job.setOutputKeyClass(DoubleWritable.class);
  	job.setOutputValueClass(DoubleWritable.class);
  	FileInputFormat.addInputPath(job, new Path(args[0]));
  	// set the HDFS path for the output
  	FileOutputFormat.setOutputPath(job, new Path(args[1]));
  	//Wait till job completion
  	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
  }