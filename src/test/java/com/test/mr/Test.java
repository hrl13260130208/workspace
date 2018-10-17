package com.test.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class Test {
	@org.junit.Test
	public void testmapper() throws IOException{
		Text value=new Text("dad:");
		MapDriver<LongWritable, Text,LongWritable, Text> mapDriver=new MapDriver<LongWritable, Text,LongWritable, Text>();
		mapDriver.withMapper(new IMapper());
		mapDriver.withInput(new LongWritable(0),value);
		mapDriver.withOutput(new LongWritable(0), new Text("null#"));
		mapDriver.runTest();
	}
	
	@org.junit.Test
	public void testReducer() throws IOException{
		Text value=new Text("null#");
		List<Text> vList=new ArrayList<Text>();
		vList.add(value);
		vList.add(value);
		
		new ReduceDriver<LongWritable, Text, LongWritable, Text>()
		.withInput(new LongWritable(1),vList)
		.withOutput(new LongWritable(1), new Text("null#null#"))
		.withReducer(new IReducer())
		.runTest();
		
	}
	
	

}
