// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.ArrayList;
import java.util.List;

/**
 * 约束放置算法输出结果封装类，对应YARN调度中约束放置算法的计算结果。
 * 类似MapReduce中Mapper/Reducer的输出收集器，算法每次运行可生成多个输出对象，
 * 通过ConstraintPlacementAlgorithmOutputCollector收集汇总结果。
 */
public class ConstraintPlacementAlgorithmOutput {

  /** 所属应用的ID */
  private final ApplicationId applicationId;

  /**
   * 构造方法，绑定所属应用
   * @param applicationId 应用ID
   */
  public ConstraintPlacementAlgorithmOutput(ApplicationId applicationId) {
    this.applicationId = applicationId;
  }

  /** 放置成功的调度请求列表 */
  private final List<PlacedSchedulingRequest> placedRequests =
      new ArrayList<>();

  /** 放置失败被拒绝的调度请求列表 */
  private final List<SchedulingRequestWithPlacementAttempt> rejectedRequests =
      new ArrayList<>();

  /**
   * 获取放置成功的调度请求列表
   * @return 放置成功请求列表
   */
  public List<PlacedSchedulingRequest> getPlacedRequests() {
    return placedRequests;
  }

  /**
   * 获取放置被拒绝的调度请求列表
   * @return 被拒绝请求列表
   */
  public List<SchedulingRequestWithPlacementAttempt> getRejectedRequests() {
    return rejectedRequests;
  }

  /**
   * 获取所属应用ID
   * @return 应用ID
   */
  public ApplicationId getApplicationId() {
    return applicationId;
  }
}