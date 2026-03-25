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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * 用于输出SequenceFile格式文件的OutputFormat实现
 * 负责将MapReduce计算结果以二进制SequenceFile格式写入文件系统
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileOutputFormat <K,V> extends FileOutputFormat<K, V> {

  /**
   * 创建并初始化SequenceFile写入器，负责处理压缩配置和文件路径处理
   * @param context 任务尝试上下文，包含任务配置和信息
   * @param keyClass 输出键的类型Class对象
   * @param valueClass 输出值的类型Class对象
   * @return 初始化完成的SequenceFile.Writer实例
   * @throws IOException 创建写入器过程中IO异常
   */
  protected SequenceFile.Writer getSequenceWriter(TaskAttemptContext context,
      Class<?> keyClass, Class<?> valueClass) 
      throws IOException {
    Configuration conf = context.getConfiguration();
	    
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    // 判断是否需要开启输出压缩
    if (getCompressOutput(context)) {
      // 获取压缩类型配置
      compressionType = getOutputCompressionType(context);
      // 获取压缩编解码器类型，默认使用DefaultCodec
      Class<?> codecClass = getOutputCompressorClass(context, 
                                                     DefaultCodec.class);
      // 通过反射实例化压缩编解码器
      codec = (CompressionCodec) 
        ReflectionUtils.newInstance(codecClass, conf);
    }
    // 获取临时输出文件路径
    Path file = getDefaultWorkFile(context, "");
    // 获取文件系统实例
    FileSystem fs = file.getFileSystem(conf);
    // 创建并返回SequenceFile写入器
    return SequenceFile.createWriter(fs, conf, file,
             keyClass,
             valueClass,
             compressionType,
             codec,
             context);
  }
  
  /**
   * 获取用于输出键值对的RecordWriter实例
   * @param context 任务尝试上下文
   * @return 适配SequenceFile写入的RecordWriter实例
   * @throws IOException 获取写入器过程中IO异常
   * @throws InterruptedException 过程被中断异常
   */
  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext context
                         ) throws IOException, InterruptedException {
    // 初始化SequenceFile写入器
    final SequenceFile.Writer out = getSequenceWriter(context,
      context.getOutputKeyClass(), context.getOutputValueClass());

    // 封装为RecordWriter返回
    return new RecordWriter<K, V>() {

        public void write(K key, V value)
          throws IOException {
          // 向SequenceFile追加写入键值对
          out.append(key, value);
        }

        public void close(TaskAttemptContext context) throws IOException { 
          // 关闭底层写入器
          out.close();
        }
      };
  }

  /**
   * 从作业配置中获取SequenceFile输出的压缩类型
   * @param job 作业上下文对象
   * @return 配置的压缩类型，默认返回RECORD级压缩
   */
  public static CompressionType getOutputCompressionType(JobContext job) {
    String val = job.getConfiguration().get(FileOutputFormat.COMPRESS_TYPE, 
                                            CompressionType.RECORD.toString());
    return CompressionType.valueOf(val);
  }
  
  /**
   * 设置SequenceFile输出的压缩类型，自动开启输出压缩
   * @param job 需要修改配置的作业对象
   * @param style 要设置的压缩类型
   */
  public static void setOutputCompressionType(Job job, 
		                                          CompressionType style) {
    // 开启输出压缩
    setCompressOutput(job, true);
    // 将压缩类型写入作业配置
    job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE, 
                               style.toString());
  }

}