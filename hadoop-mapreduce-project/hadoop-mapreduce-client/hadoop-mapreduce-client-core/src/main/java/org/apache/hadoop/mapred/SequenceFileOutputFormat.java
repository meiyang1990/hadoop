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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/** 
 * Hadoop MapReduce旧API的SequenceFile输出格式实现，将MapReduce任务输出写入SequenceFile格式文件。
 * 支持配置不同压缩类型和压缩编解码器，满足海量结构化数据输出存储需求。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileOutputFormat <K,V> extends FileOutputFormat<K, V> {

  /**
   * 获取用于写入当前任务输出的RecordWriter实例，负责创建SequenceFile写入器。
   * @param ignored 未使用的文件系统对象
   * @param job 作业配置对象JobConf
   * @param name 输出文件名
   * @param progress 进度回调对象，用于汇报写入进度
   * @return 封装了SequenceFile写入器的RecordWriter实例
   * @throws IOException 创建写入器时发生IO异常
   */
  public RecordWriter<K, V> getRecordWriter(
                                          FileSystem ignored, JobConf job,
                                          String name, Progressable progress)
    throws IOException {
    // 获取任务输出临时文件路径
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    
    FileSystem fs = file.getFileSystem(job);
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    // 检查是否需要开启输出压缩
    if (getCompressOutput(job)) {
      // 获取配置的压缩类型
      compressionType = getOutputCompressionType(job);

      // 获取配置的压缩编解码器类
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,
	  DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    }
    // 创建SequenceFile写入器
    final SequenceFile.Writer out = 
      SequenceFile.createWriter(fs, job, file,
                                job.getOutputKeyClass(),
                                job.getOutputValueClass(),
                                compressionType,
                                codec,
                                progress);

    // 返回包装后的RecordWriter
    return new RecordWriter<K, V>() {

        public void write(K key, V value)
          throws IOException {
          // 写入键值对到SequenceFile
          out.append(key, value);
        }

        public void close(Reporter reporter) throws IOException { 
          // 关闭SequenceFile写入器
          out.close();
        }
      };
  }

  /**
   * 读取指定输出目录下所有分块输出文件，返回SequenceFile读取器数组。
   * 常用于读取MapReduce作业输出结果，自动按路径名排序保证分区顺序正确。
   * @param conf 作业配置对象
   * @param dir 作业输出目录路径
   * @return 目录下所有分块输出文件对应的SequenceFile.Reader数组，按路径名排序
   * @throws IOException 读取文件时发生IO异常
   */
  public static SequenceFile.Reader[] getReaders(Configuration conf, Path dir)
    throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    Path[] names = FileUtil.stat2Paths(fs.listStatus(dir));
    
    // 对输出文件路径排序，保证哈希分区顺序正确
    Arrays.sort(names);
    
    SequenceFile.Reader[] parts = new SequenceFile.Reader[names.length];
    // 为每个输出文件创建对应的SequenceFile读取器
    for (int i = 0; i < names.length; i++) {
      parts[i] = new SequenceFile.Reader(fs, names[i], conf);
    }
    return parts;
  }

  /**
   * 从作业配置中获取SequenceFile输出的压缩类型，默认使用RECORD级压缩。
   * @param conf 作业配置对象JobConf
   * @return 配置的压缩类型，未配置时默认返回CompressionType.RECORD
   */
  public static CompressionType getOutputCompressionType(JobConf conf) {
    String val = conf.get(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.COMPRESS_TYPE, CompressionType.RECORD.toString());
    return CompressionType.valueOf(val);
  }
  
  /**
   * 向作业配置中设置SequenceFile输出的压缩类型，并自动开启输出压缩。
   * @param conf 要修改的作业配置对象JobConf
   * @param style 要设置的SequenceFile压缩类型
   */
  public static void setOutputCompressionType(JobConf conf, 
		                                          CompressionType style) {
    // 开启输出压缩
    setCompressOutput(conf, true);
    // 写入压缩类型配置
    conf.set(org.apache.hadoop.mapreduce.lib.output.
      FileOutputFormat.COMPRESS_TYPE, style.toString());
  }

}