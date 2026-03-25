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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * IFile 格式包装的Map输出抽象基类，为不同类型的Map输出流提供统一的shuffle处理框架
 * 封装了将输入流包装为IFile输入流的通用逻辑，子类只需实现具体的shuffle业务逻辑
 * 
 * @param <K> Map输出键类型
 * @param <V> Map输出值类型
 */
public abstract class IFileWrappedMapOutput<K, V> extends MapOutput<K, V> {
  private final Configuration conf;
  private final MergeManagerImpl<K, V> merger;

  /**
   * 构造IFileWrappedMapOutput实例
   * 
   * @param c Hadoop配置对象
   * @param m 归并管理器实例
   * @param mapId Map任务尝试ID
   * @param size 输出数据大小
   * @param primaryMapOutput 是否为主Map输出
   */
  public IFileWrappedMapOutput(
      Configuration c, MergeManagerImpl<K, V> m, TaskAttemptID mapId,
      long size, boolean primaryMapOutput) {
    super(mapId, size, primaryMapOutput);
    conf = c;
    merger = m;
  }

  /**
   * 获取归并管理器实例
   * 
   * @return 当前使用的归并管理器
   */
  protected MergeManagerImpl<K, V> getMerger() {
    return merger;
  }

  /**
   * 子类实现具体的shuffle处理逻辑，处理IFile格式的Map输出
   * 
   * @param host Map输出所在主机
   * @param iFileInputStream IFile格式输入流
   * @param compressedLength 压缩后数据长度
   * @param decompressedLength 解压缩后数据长度
   * @param metrics shuffle阶段指标收集器
   * @param reporter 任务报告器
   * @throws IOException 处理过程中发生IO异常时抛出
   */
  protected abstract void doShuffle(
      MapHost host, IFileInputStream iFileInputStream,
      long compressedLength, long decompressedLength,
      ShuffleClientMetrics metrics, Reporter reporter) throws IOException;

  /**
   * 通用shuffle入口方法，将原始输入流包装为IFile输入流后委托给子类处理
   * 
   * @param host Map输出所在主机
   * @param input 原始输入流
   * @param compressedLength 压缩后数据长度
   * @param decompressedLength 解压缩后数据长度
   * @param metrics shuffle阶段指标收集器
   * @param reporter 任务报告器
   * @throws IOException 处理过程中发生IO异常时抛出
   */
  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    doShuffle(host, new IFileInputStream(input, compressedLength, conf),
        compressedLength, decompressedLength, metrics, reporter);
  }
}