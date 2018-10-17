package com.test.mr;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IReducer extends Reducer<LongWritable, Text, LongWritable, Text>{

	@Override
	protected void reduce(LongWritable key, Iterable<Text> value,
			Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
		
		/*
		long lkey=key.get();
		long newKey=lkey+1;
		String string="";
		for (Text v: value){
			System.out.println(v.toString());
			String string1=v.toString()+"+reducer_";
			string=string+string1;
		}
		
		context.write(new LongWritable(newKey), new Text(string));
		*/
		String string="";
		int i=0;
		for (Text v: value){
			System.out.println(key.get()+"--------------------------"+v.toString());
			String string1=v.toString();
			string=string+string1;
			i++;
		}
//		System.out.println("reduce:"+i);
		context.write(key, new Text(string));
		
	}

	
	
}
