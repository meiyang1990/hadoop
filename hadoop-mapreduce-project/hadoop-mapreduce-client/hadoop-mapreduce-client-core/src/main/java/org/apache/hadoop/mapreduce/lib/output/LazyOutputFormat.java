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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件级注释：LazyOutputFormat 是延迟输出格式实现类，用于延迟创建输出文件，只有当任务真正输出数据时才创建输出文件，
 * 通常配合 MultipleOutputs 使用，可以还原旧Hadoop API中 MultipleTextOutputFormat 等类的行为，避免空输出文件被生成。
 * 
 * A Convenience class that creates output lazily.
 * Use in conjuction with org.apache.hadoop.mapreduce.lib.output.MultipleOutputs to recreate the
 * behaviour of org.apache.hadoop.mapred.lib.MultipleTextOutputFormat (etc) of the old Hadoop API.
 * See {@link MultipleOutputs} documentation for more information.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * 类级注释：延迟输出格式封装类，继承FilterOutputFormat，对底层真实输出格式做包装，实现延迟创建输出文件的功能。
 * 核心作用是避免没有输出数据的reduce/map任务生成空输出文件，配合多输出场景使用可以减少无用文件产生。
 * 
 * @param <K> 输出键类型
 * @param <V> 输出值类型
 */
public class LazyOutputFormat <K,V> extends FilterOutputFormat<K, V> {
  /** 配置项键：存储底层真实输出格式的类名 */
  public static String OUTPUT_FORMAT = 
    "mapreduce.output.lazyoutputformat.outputformat";
  /**
   * 函数级注释：为作业设置底层真实输出格式类，将当前LazyOutputFormat设置为作业输出格式，并保存底层输出格式到配置中。
   * @param job 作业对象，将被修改配置
   * @param theClass 底层真实输出格式类
   */
  @SuppressWarnings("unchecked")
  public static void  setOutputFormatClass(Job job, 
                                     Class<? extends OutputFormat> theClass) {
      job.setOutputFormatClass(LazyOutputFormat.class);
      job.getConfiguration().setClass(OUTPUT_FORMAT, 
          theClass, OutputFormat.class);
  }

  /**
   * 函数级注释：从作业配置中反射实例化底层真实输出格式对象，保存到baseOut成员变量。
   * @param conf 作业配置对象
   * @throws IOException 如果未配置底层输出格式则抛出异常
   */
  @SuppressWarnings("unchecked")
  private void getBaseOutputFormat(Configuration conf) 
  throws IOException {
    baseOut =  ((OutputFormat<K, V>) ReflectionUtils.newInstance(
      conf.getClass(OUTPUT_FORMAT, null), conf));
    if (baseOut == null) {
      throw new IOException("Output Format not set for LazyOutputFormat");
    }
  }

  @Override
  /**
   * 函数级注释：获取延迟RecordWriter，在首次写入数据时才会真正创建底层RecordWriter和输出文件。
   * @param context 任务尝试上下文
   * @return 延迟RecordWriter实例
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
    return new LazyRecordWriter<K, V>(baseOut, context);
  }
  
  @Override
  /**
   * 函数级注释：检查输出规格，委托给底层输出格式执行检查。
   * @param context 作业上下文
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void checkOutputSpecs(JobContext context) 
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
   super.checkOutputSpecs(context);
  }
  
  @Override
  /**
   * 函数级注释：获取输出提交器，委托给底层输出格式返回对应的输出提交器。
   * @param context 任务尝试上下文
   * @return 底层输出格式的输出提交器
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
    return super.getOutputCommitter(context);
  }
  
  /**
   * 类级注释：延迟RecordWriter实现，只有当第一次调用write方法时才会真正创建底层RecordWriter和输出文件。
   * 如果任务没有输出任何数据，则永远不会创建输出文件，从而避免生成空文件。
   * 
   * @param <K> 输出键类型
   * @param <V> 输出值类型
   */
  private static class LazyRecordWriter<K,V> extends FilterRecordWriter<K,V> {

    /** 底层输出格式实例 */
    final OutputFormat<K,V> outputFormat;
    /** 任务尝试上下文 */
    final TaskAttemptContext taskContext;

    /**
     * 函数级注释：构造延迟RecordWriter，保存上下文信息，此时不创建底层writer。
     * @param out 底层输出格式实例
     * @param taskContext 任务尝试上下文
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public LazyRecordWriter(OutputFormat<K,V> out, 
                            TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
      this.outputFormat = out;
      this.taskContext = taskContext;
    }

    @Override
    /**
     * 函数级注释：写入键值对，首次写入时先创建底层writer，然后执行写入。
     * @param key 输出键
     * @param value 输出值
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public void write(K key, V value) throws IOException, InterruptedException {
      if (rawWriter == null) {
        rawWriter = outputFormat.getRecordWriter(taskContext);
      }
      rawWriter.write(key, value);
    }

    @Override
    /**
     * 函数级注释：关闭writer，如果底层writer已创建则关闭它，否则不执行任何操作。
     * @param context 任务尝试上下文
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public void close(TaskAttemptContext context) 
    throws IOException, InterruptedException {
      if (rawWriter != null) {
        rawWriter.close(context);
      }
    }

  }
}