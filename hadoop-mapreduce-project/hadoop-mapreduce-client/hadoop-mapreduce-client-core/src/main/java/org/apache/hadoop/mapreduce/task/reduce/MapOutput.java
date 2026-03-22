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

import java.io.InputStream;
import java.io.IOException;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Map任务输出抽象基类，代表Reduce阶段需要拉取的单个Map任务的输出数据
 * 封装了Map输出的基本属性，定义了混洗、提交、中止等核心操作接口，是Shuffle阶段处理Map输出的核心抽象
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public abstract class MapOutput<K, V> {
  // 全局自增ID生成器，保证每个MapOutput实例拥有全局唯一ID
  private static AtomicInteger ID = new AtomicInteger(0);
  
  private final int id;
  private final TaskAttemptID mapId;
  private final long size;
  private final boolean primaryMapOutput;
  
  /**
   * 构造Map输出实例，初始化基本属性并分配全局唯一ID
   * @param mapId 生成该输出的Map任务尝试ID
   * @param size 输出数据大小
   * @param primaryMapOutput 是否为主要Map输出（用于推测执行场景，标识成功完成的Map输出）
   */
  public MapOutput(TaskAttemptID mapId, long size, boolean primaryMapOutput) {
    this.id = ID.incrementAndGet();
    this.mapId = mapId;
    this.size = size;
    this.primaryMapOutput = primaryMapOutput;
  }
  
  /**
   * 获取当前输出是否为推测执行场景下的主要Map输出
   * @return true表示该输出是需要使用的有效输出，false表示是推测产生的冗余输出
   */
  public boolean isPrimaryMapOutput() {
    return primaryMapOutput;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapOutput) {
      return id == ((MapOutput)obj).id;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id;
  }

  /**
   * 获取生成该输出的Map任务尝试ID
   * @return Map任务尝试ID
   */
  public TaskAttemptID getMapId() {
    return mapId;
  }

  /**
   * 获取该Map输出的数据大小
   * @return 输出数据大小，单位字节
   */
  public long getSize() {
    return size;
  }

  /**
   * 从Map节点拉取（混洗）Map输出数据，将输入流数据写入存储位置
   * @param host Map输出所在的主机信息
   * @param input 包含Map输出的输入流
   * @param compressedLength 压缩后数据长度
   * @param decompressedLength 解压缩后数据长度
   * @param metrics Shuffle阶段指标收集器，用于统计混洗性能指标
   * @param reporter 任务进度报告器，用于上报混洗进度
   * @throws IOException 拉取或写入数据失败时抛出IO异常
   */
  public abstract void shuffle(MapHost host, InputStream input,
                               long compressedLength,
                               long decompressedLength,
                               ShuffleClientMetrics metrics,
                               Reporter reporter) throws IOException;

  /**
   * 提交已完成混洗的Map输出，确认输出可用
   * @throws IOException 提交过程IO异常
   */
  public abstract void commit() throws IOException;
  
  /**
   * 中止Map输出处理，清理已拉取的临时数据
   */
  public abstract void abort();

  /**
   * 获取Map输出的描述信息，用于日志和调试
   * @return 描述字符串
   */
  public abstract String getDescription();

  public String toString() {
    return "MapOutput(" + mapId + ", " + getDescription() + ")";
  }
  
  /**
   * Map输出比较器，按照数据大小升序排序，大小相同时按ID升序排序
   * 用于Shuffle阶段对Map输出排序，通常小输出优先合并，提升合并效率
   */
  public static class MapOutputComparator<K, V> 
  implements Comparator<MapOutput<K, V>> {
    public int compare(MapOutput<K, V> o1, MapOutput<K, V> o2) {
      if (o1.id == o2.id) { 
        return 0;
      }
      
      if (o1.size < o2.size) {
        return -1;
      } else if (o1.size > o2.size) {
        return 1;
      }
      
      if (o1.id < o2.id) {
        return -1;
      } else {
        return 1;
      
      }
    }
  }
  
}