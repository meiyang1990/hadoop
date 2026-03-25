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

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.yarn.api.records.NodeReport;

/**
 * 作业节点更新事件，用于通知作业集群节点信息发生变更
 * 承载更新后的节点报告列表，驱动作业重新分配任务处理运行
 */
public class JobUpdatedNodesEvent extends JobEvent {

  private final List<NodeReport> updatedNodes;

  /**
   * 构造作业节点更新事件
   * @param jobId 目标作业ID
   * @param updatedNodes 更新后的节点报告列表
   */
  public JobUpdatedNodesEvent(JobId jobId, List<NodeReport> updatedNodes) {
    super(jobId, JobEventType.JOB_UPDATED_NODES);
    this.updatedNodes = updatedNodes;
  }

  /**
   * 获取更新后的节点报告列表
   * @return 更新后的节点报告列表
   */
  public List<NodeReport> getUpdatedNodes() {
    return updatedNodes;
  }

}