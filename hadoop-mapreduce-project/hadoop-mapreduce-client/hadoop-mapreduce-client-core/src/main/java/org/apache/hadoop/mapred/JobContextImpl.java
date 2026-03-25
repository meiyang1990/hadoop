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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Progressable;

/**
 * 兼容旧版MapReduce API的作业上下文实现类
 * 封装旧版API中作业运行所需的配置与进度上报能力，继承新版JobContextImpl适配旧版接口
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobContextImpl 
    extends org.apache.hadoop.mapreduce.task.JobContextImpl 
    implements JobContext {
  private JobConf job;
  private Progressable progress;

  /**
   * 构造旧版API作业上下文对象
   * @param conf 旧版作业配置对象
   * @param jobId 作业ID
   * @param progress 进度上报回调对象
   */
  public JobContextImpl(JobConf conf, org.apache.hadoop.mapreduce.JobID jobId, 
                 Progressable progress) {
    super(conf, jobId);
    this.job = conf;
    this.progress = progress;
  }

  /**
   * 构造旧版API作业上下文对象，使用空进度上报器
   * @param conf 旧版作业配置对象
   * @param jobId 作业ID
   */
  public JobContextImpl(JobConf conf, org.apache.hadoop.mapreduce.JobID jobId) {
    this(conf, jobId, Reporter.NULL);
  }
  
  /**
   * 获取当前作业的旧版配置对象
   * 
   * @return 旧版JobConf配置对象
   */
  public JobConf getJobConf() {
    return job;
  }
  
  /**
   * 获取进度上报机制对象，用于任务执行过程中上报进度
   * 
   * @return 进度上报回调对象
   */
  public Progressable getProgressible() {
    return progress;
  }
}