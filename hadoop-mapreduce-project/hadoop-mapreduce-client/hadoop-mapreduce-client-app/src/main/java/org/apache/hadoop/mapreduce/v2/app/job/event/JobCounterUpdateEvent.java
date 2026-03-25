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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 作业计数器更新事件，用于封装MapReduce作业运行过程中产生的计数器增量更新信息
 * 传递给作业事件处理逻辑，完成全局作业计数器的更新
 */
public class JobCounterUpdateEvent extends JobEvent {

  // 存储本次事件包含的所有计数器增量更新列表
  List<CounterIncrementalUpdate> counterUpdates = null;
  
  /**
   * 构造指定作业的计数器更新事件
   * @param jobId 目标作业ID
   */
  public JobCounterUpdateEvent(JobId jobId) {
    super(jobId, JobEventType.JOB_COUNTER_UPDATE);
    counterUpdates = new ArrayList<JobCounterUpdateEvent.CounterIncrementalUpdate>();
  }

  /**
   * 添加一个计数器增量更新条目到事件中
   * @param key 计数器枚举键
   * @param incrValue 增量值
   */
  public void addCounterUpdate(Enum<?> key, long incrValue) {
    counterUpdates.add(new CounterIncrementalUpdate(key, incrValue));
  }
  
  /**
   * 获取本次事件包含的所有计数器增量更新列表
   * @return 计数器增量更新列表
   */
  public List<CounterIncrementalUpdate> getCounterUpdates() {
    return counterUpdates;
  }
  
  /**
   * 单个计数器增量更新记录，保存单个计数器的键和增量值
   */
  public static class CounterIncrementalUpdate {
    // 计数器键，使用枚举类型标识不同计数器
    Enum<?> key;
    // 计数器增量值
    long incrValue;
    
    /**
     * 构造单个计数器增量更新记录
     * @param key 计数器键
     * @param incrValue 增量值
     */
    public CounterIncrementalUpdate(Enum<?> key, long incrValue) {
      this.key = key;
      this.incrValue = incrValue;
    }
    
    /**
     * 获取计数器键
     * @return 计数器枚举键
     */
    public Enum<?> getCounterKey() {
      return key;
    }

    /**
     * 获取计数器增量值
     * @return 增量值
     */
    public long getIncrementValue() {
      return incrValue;
    }
  }
}