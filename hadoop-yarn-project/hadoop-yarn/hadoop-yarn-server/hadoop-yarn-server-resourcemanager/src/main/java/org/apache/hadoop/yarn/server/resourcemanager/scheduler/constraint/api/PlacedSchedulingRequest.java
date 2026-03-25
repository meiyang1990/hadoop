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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.ArrayList;
import java.util.List;

/**
 * YARN调度约束模块中，封装已完成位置分配的调度请求实体类。
 * 保存原始调度请求和对应分配到的节点列表，一个分配容器对应一个节点。
 * 
 * 注意：此类调用者不应该依赖ResourceSizing中的numAllocations字段，
 * 而应该直接使用getNodes()返回的集合大小来获取实际分配数量。
 */
public class PlacedSchedulingRequest {

  /**
   * 调度请求分配重试次数，记录因集群瞬态状态（节点空间不足、用户限额超限等）
   * 提交阶段被拒绝后，分配算法尝试重新分配的次数，允许算法尝试分配到其他节点。
   */
  private int placementAttempt = 0;
  // 原始调度请求
  private final SchedulingRequest request;
  /**
   * 已分配节点列表，调度请求中每个容器对应一个节点。
   */
  private final List<SchedulerNode> nodes = new ArrayList<>();

  /**
   * 构造方法，基于原始调度请求创建已分配对象。
   * @param request 原始调度请求
   */
  public PlacedSchedulingRequest(SchedulingRequest request) {
    this.request = request;
  }

  /**
   * 获取原始调度请求。
   * @return 原始调度请求
   */
  public SchedulingRequest getSchedulingRequest() {
    return request;
  }

  /**
   * 获取已分配节点列表，列表大小等于本次请求需要分配的容器数量。
   * @return 已分配调度节点列表
   */
  public List<SchedulerNode> getNodes() {
    return nodes;
  }

  /**
   * 获取当前分配尝试次数。
   * @return 分配尝试次数
   */
  public int getPlacementAttempt() {
    return placementAttempt;
  }

  /**
   * 设置分配尝试次数。
   * @param attempt 尝试次数
   */
  public void setPlacementAttempt(int attempt) {
    this.placementAttempt = attempt;
  }

  @Override
  public String toString() {
    return "PlacedSchedulingRequest{" +
        "placementAttempt=" + placementAttempt +
        ", request=" + request +
        ", nodes=" + nodes +
        '}';
  }
}