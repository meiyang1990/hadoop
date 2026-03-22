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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

/**
 * 文件级注释：TextOutputFormat是MapReduce框架输出格式实现，用于将MapReduce计算结果输出为普通文本文件
 * 输出格式为每行一条记录，键和值使用分隔符分隔，支持输出压缩。
 * 继承自FileOutputFormat，实现基于文件的文本输出。
 * 
 * An {@link OutputFormat} that writes plain text files.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextOutputFormat<K, V> extends FileOutputFormat<K, V> {
  /** 配置项：键值分隔符，默认制表符 */
  public static String SEPARATOR = "mapreduce.output.textoutputformat.separator";
  /**
   * @deprecated Use {@link #SEPARATOR}
   * 拼写错误旧配置项，已废弃，请使用SEPARATOR
   */
  @Deprecated
  public static String SEPERATOR = SEPARATOR;

  /**
   * 行记录写入器内部类，负责将键值对按文本格式写入输出流
   * 每行一条记录，处理键值分隔和换行
   */
  protected static class LineRecordWriter<K, V>
    extends RecordWriter<K, V> {
    /** 换行符字节数组，UTF-8编码 */
    private static final byte[] NEWLINE =
      "\n".getBytes(StandardCharsets.UTF_8);

    /** 输出流对象 */
    protected DataOutputStream out;
    /** 键值分隔符字节数组，UTF-8编码 */
    private final byte[] keyValueSeparator;

    /**
     * 构造LineRecordWriter，指定输出流和键值分隔符
     * @param out 输出流
     * @param keyValueSeparator 键值分隔符字符串
     */
    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      this.keyValueSeparator =
        keyValueSeparator.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 构造LineRecordWriter，使用默认制表符作为分隔符
     * @param out 输出流
     */
    public LineRecordWriter(DataOutputStream out) {
      this(out, "\t");
    }

    /**
     * 将对象写入字节流，Text类型做特殊优化处理避免额外拷贝
     * @param o 要写入的对象
     * @throws IOException 写入失败时抛出异常
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(StandardCharsets.UTF_8));
      }
    }

    /**
     * 写入一条键值对记录到输出流
     * 处理空键/空值场景，只输出非空部分
     * @param key 记录键
     * @param value 记录值
     * @throws IOException 写入失败时抛出异常
     */
    public synchronized void write(K key, V value)
      throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      if (!nullValue) {
        writeObject(value);
      }
      out.write(NEWLINE);
    }

    /**
     * 关闭输出流
     * @param context 任务尝试上下文
     * @throws IOException 关闭失败时抛出异常
     */
    public synchronized 
    void close(TaskAttemptContext context) throws IOException {
      out.close();
    }
  }

  /**
   * 获取RecordWriter实例，根据配置创建压缩或非压缩的文本记录写入器
   * @param job 任务尝试上下文
   * @return 文本记录写入器实例
   * @throws IOException 创建输出流失败时抛出异常
   * @throws InterruptedException 线程中断时抛出异常
   */
  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext job
                         ) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    // 检查输出是否需要压缩
    boolean isCompressed = getCompressOutput(job);
    // 从配置读取键值分隔符，默认制表符
    String keyValueSeparator= conf.get(SEPARATOR, "\t");
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      // 获取压缩编解码器类，默认GzipCodec
      Class<? extends CompressionCodec> codecClass = 
        getOutputCompressorClass(job, GzipCodec.class);
      // 创建编解码器实例
      codec = ReflectionUtils.newInstance(codecClass, conf);
      // 获取压缩文件扩展名
      extension = codec.getDefaultExtension();
    }
    // 获取输出文件路径
    Path file = getDefaultWorkFile(job, extension);
    // 获取文件系统对象
    FileSystem fs = file.getFileSystem(conf);
    // 创建文件输出流
    FSDataOutputStream fileOut = fs.create(file, false);
    if (isCompressed) {
      // 压缩输出：包装压缩流后返回LineRecordWriter
      return new LineRecordWriter<>(
          new DataOutputStream(codec.createOutputStream(fileOut)),
          keyValueSeparator);
    } else {
      // 非压缩输出：直接返回LineRecordWriter
      return new LineRecordWriter<>(fileOut, keyValueSeparator);
    }
  }
}