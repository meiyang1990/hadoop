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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.*;

/** 
 * 输出纯文本格式的MapReduce输出格式，将键值对输出为文本行
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextOutputFormat<K, V> extends FileOutputFormat<K, V> {

  /**
   * 将键值对按行写入文本输出的记录写入器实现
   */
  protected static class LineRecordWriter<K, V>
    implements RecordWriter<K, V> {
    // UTF-8编码的换行符
    private static final byte[] NEWLINE =
      "\n".getBytes(StandardCharsets.UTF_8);

    // 输出流
    protected DataOutputStream out;
    // 键值分隔符的UTF-8字节数组
    private final byte[] keyValueSeparator;

    /**
     * 构造LineRecordWriter，指定输出流和键值分隔符
     * @param out 输出流
     * @param keyValueSeparator 键值之间的分隔符字符串
     */
    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      this.keyValueSeparator =
        keyValueSeparator.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 构造LineRecordWriter，默认分隔符为制表符
     * @param out 输出流
     */
    public LineRecordWriter(DataOutputStream out) {
      this(out, "\t");
    }

    /**
     * 将对象转换为字节写入输出流，对Text类型做特殊优化处理
     * @param o 要写入的对象
     * @throws IOException 写入异常直接抛出
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        // 直接复用Text内部的字节数组，避免额外拷贝
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        // 非Text类型转为字符串后按UTF-8编码写入
        out.write(o.toString().getBytes(StandardCharsets.UTF_8));
      }
    }

    /**
     * 写入一个键值对到输出文本文件
     * @param key 输出键
     * @param value 输出值
     * @throws IOException 写入异常直接抛出
     */
    public synchronized void write(K key, V value)
      throws IOException {
      // 判断键是否为空或NullWritable
      boolean nullKey = key == null || key instanceof NullWritable;
      // 判断值是否为空或NullWritable
      boolean nullValue = value == null || value instanceof NullWritable;
      // 键值都为空则不输出任何内容
      if (nullKey && nullValue) {
        return;
      }
      // 键非空则写入键内容
      if (!nullKey) {
        writeObject(key);
      }
      // 键值都非空则写入分隔符
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      // 值非空则写入值内容
      if (!nullValue) {
        writeObject(value);
      }
      // 写入换行符分隔不同记录
      out.write(NEWLINE);
    }

    /**
     * 关闭输出流
     * @param reporter 进度报告器
     * @throws IOException 关闭异常直接抛出
     */
    public synchronized void close(Reporter reporter) throws IOException {
      out.close();
    }
  }

  /**
   * 获取文本输出的记录写入器实例，根据配置决定是否压缩输出
   * @param ignored 文件系统对象（未使用）
   * @param job 作业配置对象
   * @param name 输出文件名
   * @param progress 进度回调对象
   * @return 对应的RecordWriter实例
   * @throws IOException 创建输出流或初始化压缩编码异常
   */
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored,
                                                  JobConf job,
                                                  String name,
                                                  Progressable progress)
    throws IOException {
    // 判断输出是否需要压缩
    boolean isCompressed = getCompressOutput(job);
    // 从配置中读取键值分隔符，默认使用制表符
    String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", 
                                       "\t");
    // 不压缩的情况
    if (!isCompressed) {
      // 获取任务输出文件路径
      Path file = FileOutputFormat.getTaskOutputPath(job, name);
      // 获取文件系统
      FileSystem fs = file.getFileSystem(job);
      // 创建输出流，绑定进度回调
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
    } else {
      // 获取压缩编码类，默认使用Gzip压缩
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(job, GzipCodec.class);
      // 通过反射创建压缩编码实例
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
      // 拼接压缩文件扩展名得到输出路径
      Path file = 
        FileOutputFormat.getTaskOutputPath(job, 
                                           name + codec.getDefaultExtension());
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      // 包装压缩输出流后创建LineRecordWriter
      return new LineRecordWriter<K, V>(new DataOutputStream
                                        (codec.createOutputStream(fileOut)),
                                        keyValueSeparator);
    }
  }
}