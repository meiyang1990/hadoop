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

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * 作业完成事件，用于在MapReduce作业运行完成后通知应用主服务处理后续收尾逻辑
 * 继承YARN的通用事件抽象类，携带完成作业的ID信息
 */
public class JobFinishEvent 
          extends AbstractEvent<JobFinishEvent.Type> {

  /**
   * 作业完成事件类型枚举
   */
  public enum Type {
    /** 作业状态变更为完成 */
    STATE_CHANGED
  }

  private JobId jobID;

  /**
   * 构造作业完成事件
   * @param jobID 完成的作业ID
   */
  public JobFinishEvent(JobId jobID) {
    super(Type.STATE_CHANGED);
    this.jobID = jobID;
  }

  /**
   * 获取完成作业的ID
   * @return 作业ID对象
   */
  public JobId getJobId() {
    return jobID;
  }

}