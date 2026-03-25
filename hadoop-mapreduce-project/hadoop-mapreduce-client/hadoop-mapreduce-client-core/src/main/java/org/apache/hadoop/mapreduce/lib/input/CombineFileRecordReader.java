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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.*;
import java.lang.reflect.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * CombineFileSplit的组合式RecordReader实现，可处理CombineFileSplit中多个不同文件块，为每个文件块创建对应RecordReader
 * 当一个CombineFileSplit聚合了来自多个文件的数据块时，本类支持为每个不同文件块使用独立的RecordReader进行处理
 * @see CombineFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineFileRecordReader<K, V> extends RecordReader<K, V> {

  // 构造函数参数类型签名，用于反射创建RecordReader实例
  static final Class [] constructorSignature = new Class [] 
                                         {CombineFileSplit.class,
                                          TaskAttemptContext.class,
                                          Integer.class};

  protected CombineFileSplit split;
  protected Constructor<? extends RecordReader<K,V>> rrConstructor;
  protected TaskAttemptContext context;
  
  protected int idx;
  protected long progress;
  protected RecordReader<K, V> curReader;

  /**
   * 初始化RecordReader，若当前已有子RecordReader则转发初始化调用
   * @param split 输入分片CombineFileSplit
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void initialize(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    this.split = (CombineFileSplit)split;
    this.context = context;
    if (null != this.curReader) {
      this.curReader.initialize(split, context);
    }
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // 当前无Reader或当前Reader已读完，切换到下一个Reader
    while ((curReader == null) || !curReader.nextKeyValue()) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return curReader.getCurrentKey();
  }
  
  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return curReader.getCurrentValue();
  }
  
  @Override
  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }
  
  /**
   * 获取当前读取进度，基于已处理的数据量计算
   * @return 进度值，范围0-1
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public float getProgress() throws IOException, InterruptedException {
    long subprogress = 0;    // 当前分片已处理字节数
    if (null != curReader) {
      // 索引idx比当前实际索引大1，因此取idx-1
      subprogress = (long)(curReader.getProgress() * split.getLength(idx - 1));
    }
    return Math.min(1.0f,  (progress + subprogress)/(float)(split.getLength()));
  }
  
  /**
   * 构造CombineFileRecordReader实例，为CombineFileSplit中每个块创建指定类型的RecordReader
   * @param split 组合输入分片
   * @param context 任务尝试上下文
   * @param rrClass 子RecordReader类型
   * @throws IOException IO异常
   */
  public CombineFileRecordReader(CombineFileSplit split,
                                 TaskAttemptContext context,
                                 Class<? extends RecordReader<K,V>> rrClass)
    throws IOException {
    this.split = split;
    this.context = context;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;

    try {
      // 通过反射获取子RecordReader的构造函数
      rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
      rrConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(rrClass.getName() + 
                                 " does not have valid constructor", e);
    }
    // 初始化第一个RecordReader
    initNextRecordReader();
  }
  
  /**
   * 初始化CombineFileSplit中下一个块的RecordReader
   * @return 成功初始化返回true，所有块处理完成返回false
   * @throws IOException IO异常
   */
  protected boolean initNextRecordReader() throws IOException {

    if (curReader != null) {
      // 关闭当前Reader
      curReader.close();
      curReader = null;
      if (idx > 0) {
        // 累加已完成块的长度到总进度
        progress += split.getLength(idx-1);
      }
    }

    // 所有块都处理完成，返回false
    if (idx == split.getNumPaths()) {
      return false;
    }

    // 报告任务进度，防止超时
    context.progress();

    // 创建当前索引块的RecordReader
    try {
      Configuration conf = context.getConfiguration();
      // 设置当前处理块的配置参数，供子Reader使用
      conf.set(MRJobConfig.MAP_INPUT_FILE, split.getPath(idx).toString());
      conf.setLong(MRJobConfig.MAP_INPUT_START, split.getOffset(idx));
      conf.setLong(MRJobConfig.MAP_INPUT_PATH, split.getLength(idx));

      // 反射创建子RecordReader实例
      curReader =  rrConstructor.newInstance(new Object [] 
                            {split, context, Integer.valueOf(idx)});

      if (idx > 0) {
        // 第一个Reader由MapTask负责初始化，后续Reader需要本类自行初始化
        curReader.initialize(split, context);
      }
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    // 索引自增，准备处理下一块
    idx++;
    return true;
  }
}