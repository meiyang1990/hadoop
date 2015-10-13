package com.mama.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String uri = "hdfs://localhost:9000/cmy/school.sql";
		InputStream in = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096 , false);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(in);
		}

	}

}
