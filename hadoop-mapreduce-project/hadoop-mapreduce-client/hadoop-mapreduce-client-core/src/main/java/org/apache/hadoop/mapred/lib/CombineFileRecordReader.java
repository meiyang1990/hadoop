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

import java.io.*;
import java.lang.reflect.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * 适配CombineFileSplit的通用RecordReader实现，可为CombineFileSplit中每个文件块分配对应的RecordReader
 * 一个CombineFileSplit会聚合来自多个文件的数据块，本类支持对不同文件的数据块使用不同的RecordReader处理
 * @see CombineFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineFileRecordReader<K, V> implements RecordReader<K, V> {

  // 构造方法的参数类型签名，用于反射实例化具体的RecordReader
  static final Class [] constructorSignature = new Class [] 
                                         {CombineFileSplit.class, 
                                          Configuration.class, 
                                          Reporter.class,
                                          Integer.class};

  protected CombineFileSplit split;
  protected JobConf jc;
  protected Reporter reporter;
  protected Constructor<RecordReader<K, V>> rrConstructor;
  
  protected int idx;
  protected long progress;
  protected RecordReader<K, V> curReader;
  
  /**
   * 读取下一条键值对记录
   * @param key 输出键对象
   * @param value 输出值对象
   * @return 是否成功读取到下一条记录，false表示所有数据处理完毕
   * @throws IOException 读取过程中发生IO异常
   */
  public boolean next(K key, V value) throws IOException {
    // 当前阅读器为空或当前块已读完，初始化下一个块的阅读器
    while ((curReader == null) || !curReader.next(key, value)) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }

  /**
   * 创建键对象实例，委托给当前块的实际RecordReader实现
   * @return 新建的键对象
   */
  public K createKey() {
    return curReader.createKey();
  }
  
  /**
   * 创建值对象实例，委托给当前块的实际RecordReader实现
   * @return 新建的值对象
   */
  public V createValue() {
    return curReader.createValue();
  }
  
  /**
   * 获取已处理的数据总字节数
   * @return 已处理数据的字节偏移量
   * @throws IOException 获取位置时发生IO异常
   */
  public long getPos() throws IOException {
    return progress;
  }
  
  /**
   * 关闭当前RecordReader，关闭正在使用的子阅读器
   * @throws IOException 关闭过程中发生IO异常
   */
  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }
  
  /**
   * 获取当前读取进度，基于已处理数据量计算完成比例
   * @return 0到1之间的进度值
   * @throws IOException 计算进度时发生IO异常
   */
  public float getProgress() throws IOException {
    return Math.min(1.0f,  progress/(float)(split.getLength()));
  }
  
  /**
   * 构造方法，初始化可处理CombineFileSplit的组合RecordReader
   * @param job 作业配置对象
   * @param split 组合文件输入分片
   * @param reporter 进度报告器
   * @param rrClass 每个分块使用的具体RecordReader类型
   * @throws IOException 初始化过程中发生IO异常
   */
  public CombineFileRecordReader(JobConf job, CombineFileSplit split, 
                                 Reporter reporter,
                                 Class<RecordReader<K, V>> rrClass)
    throws IOException {
    this.split = split;
    this.jc = job;
    this.reporter = reporter;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;

    try {
      // 通过反射获取RecordReader的构造方法
      rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
      rrConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(rrClass.getName() + 
                                 " does not have valid constructor", e);
    }
    // 初始化第一个分块的RecordReader
    initNextRecordReader();
  }
  
  /**
   * 初始化下一个分块的RecordReader
   * @return 是否成功初始化下一个分块的阅读器，false表示所有分块已处理完毕
   * @throws IOException 初始化过程中发生IO异常
   */
  protected boolean initNextRecordReader() throws IOException {

    if (curReader != null) {
      // 关闭当前分块的阅读器
      curReader.close();
      curReader = null;
      if (idx > 0) {
        // 累加已处理分块的数据长度，更新总进度
        progress += split.getLength(idx-1);
      }
    }

    // 所有分块处理完毕，返回false
    if (idx == split.getNumPaths()) {
      return false;
    }

    // 报告进度，防止任务超时被杀死
    reporter.progress();

    // 反射实例化当前索引分块的RecordReader
    try {
      curReader =  rrConstructor.newInstance(new Object [] 
                            {split, jc, reporter, Integer.valueOf(idx)});

      // 设置当前处理分块的配置参数，供下游使用
      jc.set(JobContext.MAP_INPUT_FILE, split.getPath(idx).toString());
      jc.setLong(JobContext.MAP_INPUT_START, split.getOffset(idx));
      jc.setLong(JobContext.MAP_INPUT_PATH, split.getLength(idx));
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    idx++;
    return true;
  }
}