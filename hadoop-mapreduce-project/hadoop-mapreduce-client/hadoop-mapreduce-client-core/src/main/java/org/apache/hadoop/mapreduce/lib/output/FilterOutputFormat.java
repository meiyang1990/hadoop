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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件级注释：MapReduce输出格式包装器基类，提供代理模式实现，用于对底层OutputFormat进行扩展增强
 * 代理所有OutputFormat核心方法，子类可以重写部分方法实现自定义逻辑，无需重写全部方法
 * 
 * FilterOutputFormat is a convenience class that wraps OutputFormat. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterOutputFormat <K,V> extends OutputFormat<K, V> {

  protected OutputFormat<K,V> baseOut;

  public FilterOutputFormat() {
    this.baseOut = null;
  }
  
  /**
   * 基于底层输出格式构造包装器
   * @param baseOut 被包装的底层OutputFormat实例
   */
  public FilterOutputFormat(OutputFormat<K,V> baseOut) {
    this.baseOut = baseOut;
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    // 代理调用底层OutputFormat获取RecordWriter
    return getBaseOut().getRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) 
  throws IOException, InterruptedException {
    // 代理调用底层OutputFormat检查输出规格
    getBaseOut().checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    // 代理调用底层OutputFormat获取输出提交器
    return getBaseOut().getOutputCommitter(context);
  }

  /**
   * 获取被包装的底层OutputFormat，校验非空
   * @return 底层OutputFormat实例
   * @throws IOException 若底层OutputFormat未设置则抛出异常
   */
  private OutputFormat<K,V> getBaseOut() throws IOException {
    if (baseOut == null) {
      throw new IOException("OutputFormat not set for FilterOutputFormat");
    }
    return baseOut;
  }
  /**
   * 内部静态类，RecordWriter的包装器基类，用于对底层RecordWriter进行扩展增强
   * 代理所有RecordWriter核心方法，子类可以重写部分方法实现自定义逻辑
   * <code>FilterRecordWriter</code> is a convenience wrapper
   * class that extends the {@link RecordWriter}.
   */

  public static class FilterRecordWriter<K,V> extends RecordWriter<K,V> {

    protected RecordWriter<K,V> rawWriter = null;

    public FilterRecordWriter() {
      rawWriter = null;
    }
    
    /**
     * 基于底层RecordWriter构造包装器
     * @param rwriter 被包装的底层RecordWriter实例
     */
    public FilterRecordWriter(RecordWriter<K,V> rwriter) {
      this.rawWriter = rwriter;
    }
    
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      // 代理调用底层RecordWriter写入键值对
      getRawWriter().write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) 
    throws IOException, InterruptedException {
      // 代理调用底层RecordWriter关闭资源
      getRawWriter().close(context);
    }
    
    /**
     * 获取被包装的底层RecordWriter，校验非空
     * @return 底层RecordWriter实例
     * @throws IOException 若底层RecordWriter未设置则抛出异常
     */
    private RecordWriter<K,V> getRawWriter() throws IOException {
      if (rawWriter == null) {
        throw new IOException("Record Writer not set for FilterRecordWriter");
      }
      return rawWriter;
    }
  }
}