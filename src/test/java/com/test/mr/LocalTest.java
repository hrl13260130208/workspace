package com.test.mr;

import java.io.IOException;
import java.nio.file.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LocalTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration=new Configuration();
		
//		configuration.set("mapreduce.framework.name", "local");
//		configuration.set("fs.defaultFS", "file:///");
//		configuration.set("yarn.resourcemanager.hostname", "local");
		
		
		Job job=Job.getInstance(configuration);
		
		
		job.setMapperClass(IMapper.class);
		job.setReducerClass(IReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
//		FileInputFormat.setMinInputSplitSize(job, 301349250);//…Ë÷√minSize
//		FileInputFormat.setMaxInputSplitSize(job, 10000);//…Ë÷√maxSize
		
		FileInputFormat.setInputPaths(job, "C:/temp/test.txt");
		FileOutputFormat.setOutputPath(job,new Path("C:/temp/part_write"));
		
		job.waitForCompletion(true);
		
		
	}
	
	
}
