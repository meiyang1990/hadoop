package com.mama.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import redis.clients.jedis.Jedis;

/**
 * @author lenovo
 * 处理用户登录日志
 */
public class LoginCount {
	
	private static String INPUT_PATH        = "/home/meiyang/hadoopInput/8888,/home/meiyang/hadoopInput/9999";//输入路径
	private static String OUTPUT_PATH       = "/home/meiyang/hadoopOutput";//输出路径
	private static String INPUT_PATH_SECOND = "/home/meiyang/hadoopOutput/";//第二次的输入路径
	
	private static String REDIS_HOST        = "127.0.0.1";//redis服务器的地址
	private static int    REDIS_PORT        = 6379;//redis服务器的端口
	private static Jedis jedis              = null;
	

	public static void main(String[] args) {

		 jedis = new Jedis(REDIS_HOST , REDIS_PORT);
		 boolean result = doJob(INPUT_PATH , OUTPUT_PATH );
		 
		 //如果作业完成，写入Redis缓存
		 if(result){
			 System.out.println("=====================");
			 
			 System.out.println(jedis.smembers("13632827437"));
		 }

	}
	
	private static void writeRedis(){
		
	}
	
	private static boolean doJob(String inputPath , String outputPath ){
		
		
		    Configuration conf = new Configuration();
		    Job job = null;
			try {
				job = new Job(conf, "Data Deduplication");
			}catch (IOException e1) {
				e1.printStackTrace();
			}
		     job.setJarByClass(LoginCount.class);
		     
		     //设置Map、Combine和Reduce处理类
		     job.setMapperClass(Map.class);
		     job.setCombinerClass(Reduce.class);
		     job.setReducerClass(Reduce.class);
		     
		     //设置输出类型
		     job.setOutputKeyClass(Text.class);
		     job.setOutputValueClass(Text.class);
		     
		     //设置输入和输出目录
		     try {
//				FileInputFormat.addInputPath(job, new Path(args[0]));
//		    	MultipleInputs.addInputPath(job, new Path("/home/meiyang/hadoopInput/8888"),TextInputFormat.class);
		    	FileInputFormat.addInputPaths(job,inputPath);
				FileOutputFormat.setOutputPath(job, new Path(outputPath));
				
//				System.exit(job.waitForCompletion(true) ? 0 : 1);
				while( !job.waitForCompletion(true) ){
				}
				//作业完成等待
				return job.waitForCompletion(true);
				
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		
	}
	
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
                
				try{
				//加入redis
				  jedis.sadd(loginName, loginTime);
				}catch(Exception e){
					e.printStackTrace();
				}
				
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
