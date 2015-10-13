package com.mama.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		  String file="hdfs://192.168.10.40:9000/test/loginLog/*";//hdfs文件 地址
		  Configuration config=new Configuration();
		
		  try {
			  FileSystem fs=FileSystem.get(URI.create(file),config);//构建FileSystem
			  InputStream is=fs.open(new Path(file));//读取文件
			IOUtils.copyBytes(is, System.out,2048, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//保存到本地  最后 关闭输入输出流

	}

}
