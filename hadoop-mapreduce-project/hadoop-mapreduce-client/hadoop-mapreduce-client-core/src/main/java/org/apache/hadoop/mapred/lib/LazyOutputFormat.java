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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件级注释：延迟输出格式实现，仅当Reduce任务有输出数据时才创建输出文件
 * 避免空输出文件占用存储，是旧MapReduce API的实现
 * 
 * A Convenience class that creates output lazily. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LazyOutputFormat<K, V> extends FilterOutputFormat<K, V> {
  /**
   * 为LazyOutputFormat设置底层实际使用的输出格式
   * @param job 作业配置对象
   * @param theClass 底层输出格式的类对象
   */
  @SuppressWarnings("unchecked")
  public static void  setOutputFormatClass(JobConf job, 
      Class<? extends OutputFormat> theClass) {
      job.setOutputFormat(LazyOutputFormat.class);
      job.setClass("mapreduce.output.lazyoutputformat.outputformat", theClass, OutputFormat.class);
  }

  /**
   * 获取延迟输出的记录写入器，延迟初始化底层输出格式
   * @param ignored 文件系统对象（未使用）
   * @param job 作业配置
   * @param name 输出文件名
   * @param progress 进度回调对象
   * @return 延迟记录写入器实例
   * @throws IOException 初始化失败时抛出IO异常
   */
  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, 
      String name, Progressable progress) throws IOException {
    if (baseOut == null) {
      getBaseOutputFormat(job);
    }
    return new LazyRecordWriter<K, V>(job, baseOut, name, progress);
  }

  /**
   * 检查输出规格，完成底层输出格式的检查
   * @param ignored 文件系统对象（未使用）
   * @param job 作业配置
   * @throws IOException 检查失败时抛出IO异常
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
  throws IOException {
    if (baseOut == null) {
      getBaseOutputFormat(job);
    }
    super.checkOutputSpecs(ignored, job);
  }

  /**
   * 从作业配置反射实例化底层实际输出格式
   * @param job 作业配置对象
   * @throws IOException 实例化失败或配置为空时抛出IO异常
   */
  @SuppressWarnings("unchecked")
  private void getBaseOutputFormat(JobConf job) throws IOException {
    baseOut = ReflectionUtils.newInstance(
        job.getClass("mapreduce.output.lazyoutputformat.outputformat", null, OutputFormat.class), 
        job); 
    if (baseOut == null) {
      throw new IOException("Ouput format not set for LazyOutputFormat");
    }
  }
  
  /**
   * 延迟记录写入器，配合LazyOutputFormat实现输出文件延迟创建
   * 仅当第一次写入数据时才真正创建底层记录写入器和输出文件
   * <code>LazyRecordWriter</code> is a convenience 
   * class that works with LazyOutputFormat.
   */

  private static class LazyRecordWriter<K,V> extends FilterRecordWriter<K,V> {

    final OutputFormat of;
    final String name;
    final Progressable progress;
    final JobConf job;

    /**
     * 构造延迟记录写入器
     * @param job 作业配置
     * @param of 底层输出格式实例
     * @param name 输出文件名
     * @param progress 进度回调对象
     * @throws IOException 构造不创建文件，不会抛出异常
     */
    public LazyRecordWriter(JobConf job, OutputFormat of, String name,
        Progressable progress)  throws IOException {
      this.of = of;
      this.job = job;
      this.name = name;
      this.progress = progress;
    }

    /**
     * 关闭底层写入器，如果写入器已创建的话
     * @param reporter 进度报告器
     * @throws IOException 关闭失败时抛出IO异常
     */
    @Override
    public void close(Reporter reporter) throws IOException {
      if (rawWriter != null) {
        rawWriter.close(reporter);
      }
    }

    /**
     * 写入键值对，第一次写入时延迟创建底层写入器
     * @param key 输出键
     * @param value 输出值
     * @throws IOException 创建或写入失败时抛出IO异常
     */
    @Override
    public void write(K key, V value) throws IOException {
      if (rawWriter == null) {
        createRecordWriter();
      }
      super.write(key, value);
    }

    /**
     * 真正创建底层记录写入器和输出文件
     * @throws IOException 创建失败时抛出IO异常
     */
    @SuppressWarnings("unchecked")
    private void createRecordWriter() throws IOException {
      FileSystem fs = FileSystem.get(job);
      rawWriter = of.getRecordWriter(fs, job, name, progress);
    }  
  }
}