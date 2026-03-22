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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapred.Task.TaskReporter;

/**
 * Map任务输出收集器接口，定义了收集Map阶段输出键值对并按分区排序输出的核心协议
 * 是旧版MapReduce API中Map任务输出处理的抽象，不同实现对应不同的输出策略（内存排序、溢写磁盘等）
 * @param <K> Map输出键类型
 * @param <V> Map输出值类型
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public interface MapOutputCollector<K, V> {
  /**
   * 初始化Map输出收集器，加载配置和准备输出资源
   * @param context 上下文对象，包含任务、配置和报告器信息
   * @throws IOException IO异常
   * @throws ClassNotFoundException 类找不到异常
   */
  public void init(Context context
                  ) throws IOException, ClassNotFoundException;
  
  /**
   * 收集Map任务生成的键值对，分配到指定分区
   * @param key Map输出键
   * @param value Map输出值
   * @param partition 分区编号，对应下游Reduce任务编号
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void collect(K key, V value, int partition
                     ) throws IOException, InterruptedException;
  
  /**
   * 关闭收集器，释放所有资源
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void close() throws IOException, InterruptedException;
    
  /**
   * 刷新所有缓冲区数据，完成所有输出排序和溢写操作
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   * @throws ClassNotFoundException 类找不到异常
   */
  public void flush() throws IOException, InterruptedException, 
                             ClassNotFoundException;

  /**
   * Map输出收集器上下文，封装初始化所需的任务、配置和状态报告信息
   */
  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  @InterfaceStability.Unstable
  public static class Context {
    private final MapTask mapTask;
    private final JobConf jobConf;
    private final TaskReporter reporter;

    /**
     * 构造上下文对象
     * @param mapTask 当前Map任务实例
     * @param jobConf 作业配置
     * @param reporter 任务状态报告器
     */
    public Context(MapTask mapTask, JobConf jobConf, TaskReporter reporter) {
      this.mapTask = mapTask;
      this.jobConf = jobConf;
      this.reporter = reporter;
    }

    /**
     * 获取当前Map任务实例
     * @return 当前Map任务
     */
    public MapTask getMapTask() {
      return mapTask;
    }

    /**
     * 获取作业配置对象
     * @return 作业配置
     */
    public JobConf getJobConf() {
      return jobConf;
    }

    /**
     * 获取任务状态报告器，用于向框架汇报任务进度
     * @return 任务报告器
     */
    public TaskReporter getReporter() {
      return reporter;
    }
  }
}