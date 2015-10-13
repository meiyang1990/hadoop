package com.mama.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RedisMapReduce {
	
	
    //map将输入中的value复制到输出数据的key上，并直接输出
    /**
     * @author meiyang
     *
     */
    public static class Map extends Mapper<LongWritable,Text,Text,Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String content = value.toString();
			StringTokenizer token = new StringTokenizer(content , "\n");
			while(token.hasMoreTokens()){
				StringTokenizer lineToken = new StringTokenizer( token.nextToken() );
				String loginName = lineToken.nextToken();
				String loginTime = lineToken.nextToken();

				context.write(new Text(loginName), new Text(loginTime));
			}
			
		}

       
    }
    
    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class Reduce extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> value,Context context)
				throws IOException, InterruptedException {
			
		       Iterator<Text> values = value.iterator();
		       StringBuffer buffer = new StringBuffer("");
		       while(values.hasNext()){
		    	   buffer.append(values.next()+",");
		       }
		       context.write(key, new Text(buffer.toString()));
			
		}
    }

}
