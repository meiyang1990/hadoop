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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件级注释：MapReduce旧API的MapFile输出格式实现，用于将MapReduce作业输出写入MapFile格式文件
 * 
 * An {@link OutputFormat} that writes {@link MapFile}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapFileOutputFormat 
extends FileOutputFormat<WritableComparable, Writable> {

  /**
   * 获取RecordWriter实例，用于输出MapFile格式记录
   * @param ignored 输入文件系统对象，本方法不使用该参数
   * @param job 作业配置对象
   * @param name 输出文件名
   * @param progress 进度回调对象
   * @return 用于写入MapFile的RecordWriter实例
   * @throws IOException 若创建写入器时发生IO异常
   */
  public RecordWriter<WritableComparable, Writable> getRecordWriter(FileSystem ignored, JobConf job,
                                      String name, Progressable progress)
    throws IOException {
    // 获取任务输出文件的路径
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    
    FileSystem fs = file.getFileSystem(job);
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    if (getCompressOutput(job)) {
      // 获取输出压缩类型
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(job);

      // 获取配置指定的压缩编解码器类
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,
	  DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    }
    
    // 创建MapFile写入器，忽略传入的progress参数由MapFile自身处理进度
    final MapFile.Writer out =
      new MapFile.Writer(job, fs, file.toString(),
                         job.getOutputKeyClass().asSubclass(WritableComparable.class),
                         job.getOutputValueClass().asSubclass(Writable.class),
                         compressionType, codec,
                         progress);

    return new RecordWriter<WritableComparable, Writable>() {

        public void write(WritableComparable key, Writable value)
          throws IOException {

          out.append(key, value);
        }

        public void close(Reporter reporter) throws IOException { out.close();}
      };
  }

  /**
   * 打开当前输出格式生成的所有MapFile输出，返回Reader数组
   * @param ignored 输入文件系统对象，本方法不使用该参数
   * @param dir 输出目录路径
   * @param conf 配置对象
   * @return 所有输出分片对应的MapFile Reader数组
   * @throws IOException 若打开文件时发生IO异常
   */
  public static MapFile.Reader[] getReaders(FileSystem ignored, Path dir,
                                            Configuration conf)
      throws IOException {
    return org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat.
      getReaders(dir, conf);
  }
    
  /**
   * 根据键从输出的MapFile中查询对应的值，使用分区器定位对应分片
   * @param <K> 键类型
   * @param <V> 值类型
   * @param readers 所有输出分片的Reader数组
   * @param partitioner 分区器，用于定位键所在的分片
   * @param key 待查询的键
   * @param value 存放查询结果的值对象
   * @return 查询到的值，若不存在返回null
   * @throws IOException 若读取MapFile时发生IO异常
   */
  public static <K extends WritableComparable, V extends Writable>
  Writable getEntry(MapFile.Reader[] readers,
                                  Partitioner<K, V> partitioner,
                                  K key,
                                  V value) throws IOException {
    int readerLength = readers.length;
    int part;
    if (readerLength <= 1) {
      part = 0;
    } else {
      part = partitioner.getPartition(key, value, readers.length);
    }
    return readers[part].get(key, value);
  }

}