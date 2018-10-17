package com.test.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IMapper extends Mapper<LongWritable, Text,LongWritable, Text> {
	private long lineNum=0;
	private int num=0;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		/*
		long lkey=key.get();
		long newKey=lkey+100000;
		String string=value.toString();
		string=string+"+map";
		context.write(new LongWritable(newKey), new Text(string));
		*/
		
		
		String line=value.toString();
		String[] string=value.toString().split(":");
		String newLine="";
		if (string.length==2) {
			newLine+=string[1]+"#";
			
		}else {
			if ( "0".equals(line)) {
				num=num+1;
				System.out.println("0 num:"+num);
				
				if (num==3) {
					lineNum=lineNum+1;
					num=0;
				}
			} else {
				newLine+="null#";
			}
		}
		System.out.println(lineNum+"================="+line);
//		wait(3000);
		context.write(new LongWritable(lineNum), new Text(newLine));
	}
	
	
	

}
