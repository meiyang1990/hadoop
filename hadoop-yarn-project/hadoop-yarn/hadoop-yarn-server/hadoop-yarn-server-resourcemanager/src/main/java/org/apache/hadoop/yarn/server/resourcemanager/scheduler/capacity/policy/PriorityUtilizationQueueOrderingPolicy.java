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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels
    .RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 容量调度器队列排序策略：基于优先级和资源利用率对队列进行排序
 * 
 * 相同优先级队列排序规则：相对已用资源利用率更低的队列优先分配资源，默认所有队列优先级为0，保持原有行为
 * 
 * 不同优先级队列排序规则：
 * - 两个队列都低于保证容量：高优先级队列优先获取资源
 * - 两个队列都达到或超过保证容量：高优先级队列优先获取资源
 * - 一个达到/超过保证容量、另一个低于保证容量：低于保证容量的队列优先获取资源
 */
public class PriorityUtilizationQueueOrderingPolicy
    implements QueueOrderingPolicy {
  private List<CSQueue> queues;
  private final boolean respectPriority;

  /**
   * 基于相对利用率和优先级比较两个队列的排序顺序，也会被抢占策略使用
   *
   * @param relativeAssigned1 队列1相对已用利用率
   * @param relativeAssigned2 队列2相对已用利用率
   * @param priority1 队列1优先级
   * @param priority2 队列2优先级
   * @return 比较结果，负数表示第一个队列优先，正数表示第二个队列优先
   */
  public static int compare(double relativeAssigned1, double relativeAssigned2,
      int priority1, int priority2) {
    if (priority1 == priority2) {
      // 相对已用利用率更低的队列优先
      return Double.compare(relativeAssigned1, relativeAssigned2);
    } else{
      // 优先级不同时的比较逻辑
      if ((relativeAssigned1 < 1.0f && relativeAssigned2 < 1.0f) || (
          relativeAssigned1 >= 1.0f && relativeAssigned2 >= 1.0f)) {
        // 两个队列都低于保证容量，或都达到/超过保证容量，高优先级队列优先
        return Integer.compare(priority2, priority1);
      } else{
        // 一个达到/超过保证容量，另一个低于，低于保证容量的队列优先
        return Double.compare(relativeAssigned1, relativeAssigned2);
      }
    }
  }

  /**
   * 同时考虑优先级和资源利用率的队列比较器
   */
  final public class PriorityQueueComparator
      implements Comparator<PriorityQueueResourcesForSorting> {

    final private String partition;

    public PriorityQueueComparator(String partition) {
      this.partition = partition;
    }

    @Override
    public int compare(PriorityQueueResourcesForSorting q1Sort,
        PriorityQueueResourcesForSorting q2Sort) {
      // 先比较队列对当前节点标签分区的访问权限
      int rc = compareQueueAccessToPartition(
          q1Sort.nodeLabelAccessible,
          q2Sort.nodeLabelAccessible);
      if (0 != rc) {
        return rc;
      }

      float q1AbsCapacity = q1Sort.absoluteCapacity;
      float q2AbsCapacity = q2Sort.absoluteCapacity;

      // q1绝对容量大于0，q2等于0，优先分配q1
      if (Float.compare(q1AbsCapacity, 0f) > 0 && Float.compare(q2AbsCapacity,
          0f) == 0) {
        return -1;
      // q2绝对容量大于0，q1等于0，优先分配q2
      } else if (Float.compare(q2AbsCapacity, 0f) > 0 && Float.compare(
          q1AbsCapacity, 0f) == 0) {
        return 1;
      } else if (Float.compare(q1AbsCapacity, 0f) == 0 && Float.compare(
          q2AbsCapacity, 0f) == 0) {
        // 两个队列绝对容量都为0，使用优先级和绝对已用容量排序
        float used1 = q1Sort.absoluteUsedCapacity;
        float used2 = q2Sort.absoluteUsedCapacity;

        return compare(q1Sort, q2Sort, used1, used2,
            q1Sort.priority.
                getPriority(), q2Sort.priority.getPriority());
      } else{
        // 两个队列绝对容量都大于0，使用相对利用率和优先级排序
        float used1 = q1Sort.usedCapacity;
        float used2 = q2Sort.usedCapacity;

        return compare(q1Sort, q2Sort, used1, used2,
            q1Sort.priority.getPriority(),
            q2Sort.priority.getPriority());
      }
    }

    private int compare(PriorityQueueResourcesForSorting q1Sort,
        PriorityQueueResourcesForSorting q2Sort, float q1Used,
                        float q2Used, int q1Prior, int q2Prior) {

      int p1 = 0;
      int p2 = 0;
      // 如果开启优先级尊重，使用队列实际优先级，否则都按0处理
      if (respectPriority) {
        p1 = q1Prior;
        p2 = q2Prior;
      }

      int rc = PriorityUtilizationQueueOrderingPolicy.compare(q1Used, q2Used,
          p1, p2);

      // 利用率和优先级相同，配置最小资源更大的队列优先
      if (0 == rc) {
        Resource minEffRes1 =
            q1Sort.configuredMinResource;
        Resource minEffRes2 =
            q2Sort.configuredMinResource;
        if (!minEffRes1.equals(Resources.none()) || !minEffRes2.equals(
            Resources.none())) {
          return minEffRes2.compareTo(minEffRes1);
        }

        // 最小资源也相同，绝对容量更大的队列优先
        float abs1 = q1Sort.absoluteCapacity;
        float abs2 = q2Sort.absoluteCapacity;
        return Float.compare(abs2, abs1);
      }

      return rc;
    }

    private int compareQueueAccessToPartition(boolean q1Accessible, boolean q2Accessible) {
      // 默认分区所有队列都有权限，直接返回相等
      if (StringUtils.equals(partition, RMNodeLabelsManager.NO_LABEL)) {
        return 0;
      }

      /*
       * 检查对指定分区的访问权限，有权限的队列优先于无权限的
       */
      if (q1Accessible && !q2Accessible) {
        return -1;
      } else if (!q1Accessible && q2Accessible) {
        return 1;
      }

      // 都有权限或都无权限，返回相等
      return 0;
    }
  }

  /**
   * 存储队列排序前的快照信息，避免排序过程中队列状态变化
   */
  public static class PriorityQueueResourcesForSorting {
    private final float absoluteUsedCapacity;
    private final float usedCapacity;
    private final Resource configuredMinResource;
    private final float absoluteCapacity;
    private final Priority priority;
    private final boolean nodeLabelAccessible;
    private final CSQueue queue;

    PriorityQueueResourcesForSorting(CSQueue queue, String partition) {
      this.queue = queue;
      this.absoluteUsedCapacity =
          queue.getQueueCapacities().
              getAbsoluteUsedCapacity(partition);
      this.usedCapacity =
          queue.getQueueCapacities().
              getUsedCapacity(partition);
      this.absoluteCapacity =
          queue.getQueueCapacities().
              getAbsoluteCapacity(partition);
      this.configuredMinResource =
          queue.getQueueResourceQuotas().
              getConfiguredMinResource(partition);
      this.priority = queue.getPriority();
      // 判断队列是否可以访问当前节点标签分区
      this.nodeLabelAccessible = queue.getAccessibleNodeLabels() != null &&
          queue.getAccessibleNodeLabels().contains(partition) ||
          queue.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY);
    }

    static PriorityQueueResourcesForSorting create(CSQueue queue, String partition) {
      return new PriorityQueueResourcesForSorting(queue, partition);
    }

    public CSQueue getQueue() {
      return queue;
    }
  }

  /**
   * 构造函数，指定是否尊重队列优先级
   * @param respectPriority 是否尊重队列优先级
   */
  public PriorityUtilizationQueueOrderingPolicy(boolean respectPriority) {
    this.respectPriority = respectPriority;
  }

  @Override
  public void setQueues(List<CSQueue> queues) {
    this.queues = queues;
  }

  @Override
  public Iterator<CSQueue> getAssignmentIterator(String partition) {
    // 拷贝队列列表保证线程安全，对队列快照排序，避免破坏TimSort前置条件，详见YARN-10178
    return new ArrayList<>(queues).stream()
        .map(queue -> PriorityQueueResourcesForSorting.create(queue, partition))
        .sorted(new PriorityQueueComparator(partition))
        .map(PriorityQueueResourcesForSorting::getQueue)
        .collect(Collectors.toList()).iterator();
  }

  @Override
  public String getConfigName() {
    if (respectPriority) {
      return CapacitySchedulerConfiguration.
          QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY;
    } else{
      return CapacitySchedulerConfiguration.
          QUEUE_UTILIZATION_ORDERING_POLICY;
    }
  }

  @VisibleForTesting
  public List<CSQueue> getQueues() {
    return queues;
  }
}