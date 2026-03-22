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

/**
 * 过滤器模式的OutputFormat包装基类，为扩展自定义输出格式提供便捷的委托封装
 * 该类将所有操作委托给底层基础OutputFormat，子类可以只重写需要自定义的方法
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterOutputFormat<K, V> implements OutputFormat<K, V> {

  protected OutputFormat<K,V> baseOut;

  /**
   * 构造空的FilterOutputFormat，需要后续设置底层输出格式
   */
  public FilterOutputFormat () {
    this.baseOut = null;
  }

  /**
   * 基于指定的底层输出格式构造FilterOutputFormat
   * @param out 底层被包装的输出格式实例
   */
  public FilterOutputFormat (OutputFormat<K,V> out) {
    this.baseOut = out;
  }

  /**
   * 获取RecordWriter，委托给底层输出格式实现
   */
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, 
      String name, Progressable progress) throws IOException {
    return getBaseOut().getRecordWriter(ignored, job, name, progress);
  }

  /**
   * 检查输出规范，委托给底层输出格式实现
   */
  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
  throws IOException {
    getBaseOut().checkOutputSpecs(ignored, job);
  }
  
  /**
   * 获取底层基础输出格式，空校验
   * @return 底层输出格式实例
   * @throws IOException 如果底层输出格式未设置则抛出异常
   */
  private OutputFormat<K,V> getBaseOut() throws IOException {
    if (baseOut == null) {
      throw new IOException("Outputformat not set for FilterOutputFormat");
    }
    return baseOut;
  }

  /**
   * 过滤器模式的RecordWriter包装基类，为扩展自定义写入逻辑提供便捷的委托封装
   * 将所有操作委托给底层原生RecordWriter，子类可以只重写需要自定义的方法
   */
  public static class FilterRecordWriter<K,V> implements RecordWriter<K,V> {

    protected RecordWriter<K,V> rawWriter = null;

    /**
     * 构造空的FilterRecordWriter，需要后续设置底层写入器
     * @throws IOException 无实际异常，仅满足方法签名要求
     */
    public FilterRecordWriter() throws IOException {
      rawWriter = null;
    }

    /**
     * 基于指定的底层写入器构造FilterRecordWriter
     * @param rawWriter 底层被包装的原始RecordWriter实例
     * @throws IOException 无实际异常，仅满足方法签名要求
     */
    public FilterRecordWriter(RecordWriter<K,V> rawWriter)  throws IOException {
      this.rawWriter = rawWriter;
    }

    /**
     * 关闭写入器，委托给底层写入器实现
     */
    public void close(Reporter reporter) throws IOException {
      getRawWriter().close(reporter);
    }

    /**
     * 写入键值对，委托给底层写入器实现
     */
    public void write(K key, V value) throws IOException {
      getRawWriter().write(key, value);
    }
    
    /**
     * 获取底层原始写入器，空校验
     * @return 底层原始写入器实例
     * @throws IOException 如果底层写入器未设置则抛出异常
     */
    private RecordWriter<K,V> getRawWriter() throws IOException {
      if (rawWriter == null) {
        throw new IOException ("Record Writer not set for FilterRecordWriter");
      }
      return rawWriter;
    }
  }

}