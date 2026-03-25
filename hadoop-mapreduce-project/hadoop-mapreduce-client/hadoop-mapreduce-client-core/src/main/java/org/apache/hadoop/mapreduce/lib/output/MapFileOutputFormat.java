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
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.PathFilter;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/** 
 * MapFile 格式输出的 OutputFormat 实现，用于将 MapReduce 输出写入 Hadoop MapFile 文件。
 * MapFile 是排序键值对文件，支持按键快速查找，适合需要随机访问输出结果的场景。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapFileOutputFormat 
    extends FileOutputFormat<WritableComparable<?>, Writable> {

  /**
   * 获取 MapFile 格式的 RecordWriter，用于写入任务输出
   * @param context 任务尝试上下文，包含配置和输出信息
   * @return 适配 MapFile 的 RecordWriter
   * @throws IOException 若文件创建或初始化失败抛出异常
   */
  public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(
      TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    // 检查是否需要输出压缩
    if (getCompressOutput(context)) {
      // 获取压缩类型配置
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);

      // 获取压缩编解码器类
      Class<?> codecClass = getOutputCompressorClass(context,
	                          DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    // 获取任务输出文件路径
    Path file = getDefaultWorkFile(context, "");
    FileSystem fs = file.getFileSystem(conf);
    // 创建 MapFile.Writer，忽略进度统计（MapFile写入是本地操作）
    final MapFile.Writer out =
      new MapFile.Writer(conf, fs, file.toString(),
        context.getOutputKeyClass().asSubclass(WritableComparable.class),
        context.getOutputValueClass().asSubclass(Writable.class),
        compressionType, codec, context);

    // 返回RecordWriter实现
    return new RecordWriter<WritableComparable<?>, Writable>() {
        public void write(WritableComparable<?> key, Writable value)
            throws IOException {
          // 追加键值对到MapFile
          out.append(key, value);
        }

        public void close(TaskAttemptContext context) throws IOException { 
          // 关闭MapFile写入器
          out.close();
        }
      };
  }

  /**
   * 打开当前输出格式生成的所有MapFile分片，返回读取器数组
   * @param dir 输出目录路径
   * @param conf 配置对象
   * @return 所有分片的MapFile读取器数组，按分片编号排序
   * @throws IOException 若读取文件列表或打开分片失败抛出异常
   */
  public static MapFile.Reader[] getReaders(Path dir,
      Configuration conf) throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    // 路径过滤器：过滤掉隐藏文件和临时文件（以_或.开头）
    PathFilter filter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        String name = path.getName();
        if (name.startsWith("_") || name.startsWith("."))
          return false;
        return true;
      }
    };
    // 获取过滤后的分片路径列表
    Path[] names = FileUtil.stat2Paths(fs.listStatus(dir, filter));

    // 对分片路径排序，保证哈希分区能正确对应分片
    Arrays.sort(names);
    
    // 创建对应每个分片的读取器
    MapFile.Reader[] parts = new MapFile.Reader[names.length];
    for (int i = 0; i < names.length; i++) {
      parts[i] = new MapFile.Reader(fs, names[i].toString(), conf);
    }
    return parts;
  }
    
  /**
   * 根据键查找对应的记录，根据分区规则选择对应分片读取器查询
   * @param readers 所有分片的读取器数组
   * @param partitioner 分区器，用于确定键所在的分片索引
   * @param key 待查找的键
   * @param value 存储查找结果值的对象
   * @return 若找到返回值对象，否则返回null
   * @throws IOException 若读取MapFile失败抛出异常
   */
  public static <K extends WritableComparable<?>, V extends Writable>
      Writable getEntry(MapFile.Reader[] readers, 
      Partitioner<K, V> partitioner, K key, V value) throws IOException {
    int readerLength = readers.length;
    int part;
    // 只有一个分片时直接命中第一个分片
    if (readerLength <= 1) {
      part = 0;
    } else {
      // 使用分区器计算键所在分片索引
      part = partitioner.getPartition(key, value, readers.length);
    }
    // 从对应分片读取器中获取记录
    return readers[part].get(key, value);
  }
}