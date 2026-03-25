// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools;


import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * HDFS 文件拼接命令行工具，用于将多个源HDFS文件拼接合并到目标文件中
 * 属于HDFS命令行工具，仅在客户端使用，供用户通过命令行直接调用拼接操作
 */
@InterfaceAudience.Private
public class HDFSConcat {
  private final static String def_uri = "hdfs://localhost:9000";

  /**
   * 命令行入口方法，解析参数并执行HDFS文件拼接操作
   * @param args 命令行参数，第一个参数为目标文件路径，后续参数为待拼接的源文件路径
   * @throws IOException 访问HDFS时发生IO异常
   */
  public static void main(String... args) throws IOException {

    // 参数个数不足时输出使用说明并退出
    if(args.length < 2) {
      System.err.println("Usage HDFSConcat target srcs..");
      System.exit(0);
    }
    
    Configuration conf = new Configuration();
    // 获取默认文件系统地址，使用配置不存在时使用默认地址
    String uri = conf.get("fs.default.name", def_uri);
    Path path = new Path(uri);
    // 获取HDFS分布式文件系统实例
    DistributedFileSystem dfs = 
      (DistributedFileSystem)FileSystem.get(path.toUri(), conf);
    
    // 构造源文件路径数组
    Path [] srcs = new Path[args.length-1];
    for(int i=1; i<args.length; i++) {
      srcs[i-1] = new Path(args[i]);
    }
    // 调用HDFS concat接口执行拼接操作
    dfs.concat(new Path(args[0]), srcs);
  }

}