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

import org.apache.hadoop.yarn.api.records.SchedulingRequest;

import java.util.Collection;

/**
 * 约束放置算法输入接口，封装YARN容器放置约束算法所需的输入数据
 * 至少需要包含待调度的调度请求集合
 */
public interface ConstraintPlacementAlgorithmInput {

  /**
   * 获取待调度的调度请求集合
   * @return 调度请求集合
   */
  Collection<SchedulingRequest> getSchedulingRequests();

}