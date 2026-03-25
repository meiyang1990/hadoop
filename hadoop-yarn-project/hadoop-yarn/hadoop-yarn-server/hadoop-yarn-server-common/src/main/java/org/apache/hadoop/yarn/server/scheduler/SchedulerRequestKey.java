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

package org.apache.hadoop.yarn.server.scheduler;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;

/**
 * YARN调度器待处理请求的复合唯一键，用于标识和排序调度请求。
 * 目前包含优先级、分配请求ID，容器更新请求还会包含待更新容器ID。
 */
public class SchedulerRequestKey implements
    Comparable<SchedulerRequestKey> {

  private final Priority priority;
  private final long allocationRequestId;
  private final ContainerId containerToUpdate;

  /**
   * 从ResourceRequest构造调度请求键工厂方法。
   * @param req 资源请求对象
   * @return 构造完成的调度请求键
   */
  public static SchedulerRequestKey create(ResourceRequest req) {
    return new SchedulerRequestKey(req.getPriority(),
        req.getAllocationRequestId(), null);
  }

  /**
   * 从SchedulingRequest构造调度请求键工厂方法。
   * @param req 调度请求对象
   * @return 构造完成的调度请求键
   */
  public static SchedulerRequestKey create(SchedulingRequest req) {
    return new SchedulerRequestKey(req.getPriority(),
        req.getAllocationRequestId(), null);
  }

  /**
   * 从UpdateContainerRequest构造容器更新请求的调度请求键。
   * @param req 容器更新请求对象
   * @param schedulerRequestKey 原始调度请求键
   * @return 构造完成的调度请求键
   */
  public static SchedulerRequestKey create(UpdateContainerRequest req,
      SchedulerRequestKey schedulerRequestKey) {
    return new SchedulerRequestKey(schedulerRequestKey.getPriority(),
        schedulerRequestKey.getAllocationRequestId(), req.getContainerId());
  }

  /**
   * 从已分配容器提取对应的调度请求键。
   * @param container 已分配容器对象
   * @return 提取后的调度请求键
   */
  public static SchedulerRequestKey extractFrom(Container container) {
    return new SchedulerRequestKey(container.getPriority(),
        container.getAllocationRequestId(), null);
  }

  /**
   * 构造调度请求键。
   * @param priority 请求优先级
   * @param allocationRequestId 分配请求ID
   * @param containerToUpdate 待更新容器ID，普通请求传null
   */
  public SchedulerRequestKey(Priority priority, long allocationRequestId,
      ContainerId containerToUpdate) {
    this.priority = priority;
    this.allocationRequestId = allocationRequestId;
    this.containerToUpdate = containerToUpdate;
  }

  /**
   * 获取请求优先级。
   * @return 请求优先级
   */
  public Priority getPriority() {
    return priority;
  }

  /**
   * 获取分配请求ID。
   * @return 分配请求ID
   */
  public long getAllocationRequestId() {
    return allocationRequestId;
  }

  /**
   * 获取待更新容器ID。
   * @return 待更新容器ID，普通请求返回null
   */
  public ContainerId getContainerToUpdate() {
    return containerToUpdate;
  }

  @Override
  public int compareTo(SchedulerRequestKey o) {
    // 处理比较对象为null的情况
    if (o == null) {
      return (priority != null) ? -1 : 0;
    } else {
      if (priority == null) {
        return 1;
      }
    }

    // 容器更新请求优先级高于普通新分配请求
    if (this.containerToUpdate == null && o.containerToUpdate != null) {
      return -1;
    }
    if (this.containerToUpdate != null && o.containerToUpdate == null) {
      return 1;
    }

    // 先按优先级排序，优先级数值越大优先级越高，因此用对方和当前比较
    int priorityCompare = o.getPriority().compareTo(priority);
    // 优先级不同直接返回比较结果
    if (priorityCompare != 0) {
      return priorityCompare;
    }
    // 优先级相同则按分配请求ID排序
    int allocReqCompare = Long.compare(
        allocationRequestId, o.getAllocationRequestId());

    if (allocReqCompare != 0) {
      return allocReqCompare;
    }

    // 都是容器更新请求，按容器ID排序
    if (this.containerToUpdate != null && o.containerToUpdate != null) {
      return (this.containerToUpdate.compareTo(o.containerToUpdate));
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    // 同一对象直接返回相等
    if (this == o) {
      return true;
    }
    // 类型不同直接返回不相等
    if (!(o instanceof SchedulerRequestKey)) {
      return false;
    }

    SchedulerRequestKey that = (SchedulerRequestKey) o;

    // 依次比较分配请求ID、优先级、待更新容器ID
    if (getAllocationRequestId() != that.getAllocationRequestId()) {
      return false;
    }
    if (!getPriority().equals(that.getPriority())) {
      return false;
    }
    return containerToUpdate != null ?
        containerToUpdate.equals(that.containerToUpdate) :
        that.containerToUpdate == null;
  }

  @Override
  public int hashCode() {
    // 按权重计算哈希值
    int result = priority != null ? priority.hashCode() : 0;
    result = 31 * result + (int) (allocationRequestId ^ (allocationRequestId
        >>> 32));
    result = 31 * result + (containerToUpdate != null ? containerToUpdate
        .hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SchedulerRequestKey{" +
        "priority=" + priority +
        ", allocationRequestId=" + allocationRequestId +
        ", containerToUpdate=" + containerToUpdate +
        '}';
  }
}