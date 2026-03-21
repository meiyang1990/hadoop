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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.List;

/**
 * YARN资源调度器资源提交请求容器，封装一轮调度中所有资源分配、预留和释放请求
 * 记录本次调度操作需要修改的所有资源变更信息，统一提交执行
 * @param <A> 调度应用尝试类型
 * @param <N> 调度节点类型
 */
public class ResourceCommitRequest<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  // 待分配的新容器列表
  private List<ContainerAllocationProposal<A, N>> containersToAllocate =
      Collections.emptyList();

  // 待预留的新容器列表
  private List<ContainerAllocationProposal<A, N>> containersToReserve =
      Collections.emptyList();;

  // 待无条件释放的容器列表
  private List<SchedulerContainer<A, N>> toReleaseContainers =
      Collections.emptyList();

  // 本次请求累计分配资源总量
  private Resource totalAllocatedResource;
  // 本次请求累计预留资源总量
  private Resource totalReservedResource;
  // 本次请求累计释放资源总量
  private Resource totalReleasedResource;

  /**
   * 构造资源提交请求，初始化后自动计算各类资源总量
   * @param containersToAllocate 待分配容器列表
   * @param containersToReserve 待预留容器列表
   * @param toReleaseContainers 待释放容器列表
   */
  public ResourceCommitRequest(
      List<ContainerAllocationProposal<A, N>> containersToAllocate,
      List<ContainerAllocationProposal<A, N>> containersToReserve,
      List<SchedulerContainer<A, N>> toReleaseContainers) {
    if (null != containersToAllocate) {
      this.containersToAllocate = containersToAllocate;
    }
    if (null != containersToReserve) {
      this.containersToReserve = containersToReserve;
    }
    if (null != toReleaseContainers) {
      this.toReleaseContainers = toReleaseContainers;
    }

    // 初始化分配资源总量为0
    totalAllocatedResource = Resources.createResource(0);
    // 初始化预留资源总量为0
    totalReservedResource = Resources.createResource(0);

    /*
     * For total-release resource, it has two parts:
     * 1) Unconditional release: for example, an app reserved a container,
     *    but the app doesn't has any pending resource.
     * 2) Conditional release: for example, reservation continuous looking, or
     *    Lazy preemption -- which we need to kill some resource to allocate
     *    or reserve the new container.
     *
     * For the 2nd part, it is inside:
     * ContainerAllocationProposal#toRelease, which means we will kill/release
     * these containers to allocate/reserve the given container.
     *
     * So we need to account both of conditional/unconditional to-release
     * containers to the total release-able resource.
     */
    // 初始化释放资源总量为0
    totalReleasedResource = Resources.createResource(0);

    // 遍历所有待分配容器，统计资源总量
    for (ContainerAllocationProposal<A,N> c : this.containersToAllocate) {
      // 累加本次分配资源
      Resources.addTo(totalAllocatedResource,
          c.getAllocatedOrReservedResource());
      // 累加分配请求附带的待释放容器资源
      for (SchedulerContainer<A,N> r : c.getToRelease()) {
        Resources.addTo(totalReleasedResource,
            r.getRmContainer().getAllocatedOrReservedResource());
      }
    }

    // 遍历所有待预留容器，统计资源总量
    for (ContainerAllocationProposal<A,N> c : this.containersToReserve) {
      // 累加本次预留资源
      Resources.addTo(totalReservedResource,
          c.getAllocatedOrReservedResource());
      // 累加预留请求附带的待释放容器资源
      for (SchedulerContainer<A,N> r : c.getToRelease()) {
        Resources.addTo(totalReleasedResource,
            r.getRmContainer().getAllocatedOrReservedResource());
      }
    }

    // 累加无条件释放的容器资源
    for (SchedulerContainer<A,N> r : this.toReleaseContainers) {
      Resources.addTo(totalReleasedResource,
          r.getRmContainer().getAllocatedOrReservedResource());
    }
  }

  public List<ContainerAllocationProposal<A, N>> getContainersToAllocate() {
    return containersToAllocate;
  }

  public List<ContainerAllocationProposal<A, N>> getContainersToReserve() {
    return containersToReserve;
  }

  public List<SchedulerContainer<A, N>> getContainersToRelease() {
    return toReleaseContainers;
  }

  public Resource getTotalAllocatedResource() {
    return totalAllocatedResource;
  }

  public Resource getTotalReservedResource() {
    return totalReservedResource;
  }

  public Resource getTotalReleasedResource() {
    return totalReleasedResource;
  }

  /*
   * Util functions to make your life easier
   */
  /**
   * 检查本次请求是否包含分配或预留操作
   * @return true 存在待分配/预留资源，否则false
   */
  public boolean anythingAllocatedOrReserved() {
    return (!containersToAllocate.isEmpty()) || (!containersToReserve
        .isEmpty());
  }

  /**
   * 获取第一个待分配或预留容器，用于快速获取节点信息等场景
   * @return 第一个容器提案，无则返回null
   */
  public ContainerAllocationProposal<A, N> getFirstAllocatedOrReservedContainer() {
    ContainerAllocationProposal<A, N> c = null;
    if (!containersToAllocate.isEmpty()) {
      c = containersToAllocate.get(0);
    }
    if (c == null && !containersToReserve.isEmpty()) {
      c = containersToReserve.get(0);
    }

    return c;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("New " + getClass().getName() + ":" + "\n");
    if (null != containersToAllocate && !containersToAllocate.isEmpty()) {
      sb.append("\t ALLOCATED=" + containersToAllocate.toString());
    }
    if (null != containersToReserve && !containersToReserve.isEmpty()) {
      sb.append("\t RESERVED=" + containersToReserve.toString());
    }
    if (null != toReleaseContainers && !toReleaseContainers.isEmpty()) {
      sb.append("\t RELEASED=" + toReleaseContainers.toString());
    }
    return sb.toString();
  }
}