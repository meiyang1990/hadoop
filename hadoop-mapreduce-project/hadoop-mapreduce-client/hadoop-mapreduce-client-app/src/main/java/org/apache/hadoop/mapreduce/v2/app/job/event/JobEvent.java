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

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 封装MapReduce作业相关事件的基类，所有作业级别事件都继承此类
 * 用于在ApplicationMaster内部传递作业状态变更通知
 */
public class JobEvent extends AbstractEvent<JobEventType> {

  // 关联的作业ID
  private JobId jobID;

  /**
   * 构造作业事件实例
   * @param jobID 关联的作业ID
   * @param type 作业事件类型
   */
  public JobEvent(JobId jobID, JobEventType type) {
    super(type);
    this.jobID = jobID;
  }

  /**
   * 获取当前事件关联的作业ID
   * @return 作业ID对象
   */
  public JobId getJobId() {
    return jobID;
  }

}