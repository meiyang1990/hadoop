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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计算每个队列需要被抢占的资源总量，供 {@link PreemptionCandidatesSelector} 使用。
 */
public class AbstractPreemptableResourceCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractPreemptableResourceCalculator.class);

  // 抢占上下文，保存全局抢占相关信息
  protected final CapacitySchedulerPreemptionContext context;
  // 资源计算器，用于资源比较和计算
  protected final ResourceCalculator rc;
  // 是否是预留资源抢占候选选择器
  protected boolean isReservedPreemptionCandidatesSelector;
  // 步长因子，用于资源归一化计算
  private Resource stepFactor;
  // 是否允许所有队列满足保证后仍进行队列间资源平衡抢占
  private boolean allowQueuesBalanceAfterAllQueuesSatisfied;

  /**
   * 临时队列按需求优先级比较器，用于优先分配资源给更缺资源的队列。
   */
  static class TQComparator implements Comparator<TempQueuePerPartition> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    TQComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempQueuePerPartition tq1, TempQueuePerPartition tq2) {
      double assigned1 = getIdealPctOfGuaranteed(tq1);
      double assigned2 = getIdealPctOfGuaranteed(tq2);

      return PriorityUtilizationQueueOrderingPolicy.compare(assigned1,
          assigned2, tq1.relativePriority, tq2.relativePriority);
    }

    // 计算理想分配占保证容量的比例
    // 保证容量为0的队列被认为是超配额最多，排到最后分配
    private double getIdealPctOfGuaranteed(TempQueuePerPartition q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(rc, clusterRes, q.getGuaranteed(),
          Resources.none())) {
        pctOver = Resources.divide(rc, clusterRes, q.idealAssigned,
            q.getGuaranteed());
      }
      return (pctOver);
    }
  }

  /**
   * 归一化计算元组，保存分子分母资源，用于计算归一化比例。
   */
  private static class NormalizationTuple {
    private Resource numerator;
    private Resource denominator;

    NormalizationTuple(Resource numer, Resource denom) {
      this.numerator = numer;
      this.denominator = denom;
    }

    long getNumeratorValue(int i) {
      return numerator.getResourceInformation(i).getValue();
    }

    long getDenominatorValue(int i) {
      String nUnits = numerator.getResourceInformation(i).getUnits();
      ResourceInformation dResourceInformation = denominator
          .getResourceInformation(i);
      return UnitsConversionUtil.convert(
          dResourceInformation.getUnits(), nUnits, dResourceInformation.getValue());
    }

    float getNormalizedValue(int i) {
      long nValue = getNumeratorValue(i);
      long dValue = getDenominatorValue(i);
      return dValue == 0 ? 0.0f : (float) nValue / dValue;
    }
  }

  /**
   * 抢占资源计算器构造函数。
   *
   * @param preemptionContext 抢占上下文
   * @param isReservedPreemptionCandidatesSelector
   *          由不同候选选择器实现设置，详情参考 TempQueuePerPartition#offer
   * @param allowQueuesBalanceAfterAllQueuesSatisfied
   *          当所有请求队列都已满足或超过保证容量时，是否允许从超配额队列抢占资源进行平衡。
   *          例如：root下有10个队列，每个保证容量都是10%，
   *          实际只有两个队列在使用资源，queueA占10%，queueB占90%。所有队列都满足保证容量，但分配不公平，
   *          该配置用于控制是否允许这种情况下抢占平衡，默认不允许。
   *
   */
  public AbstractPreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector,
      boolean allowQueuesBalanceAfterAllQueuesSatisfied) {
    context = preemptionContext;
    rc = preemptionContext.getResourceCalculator();
    this.isReservedPreemptionCandidatesSelector =
        isReservedPreemptionCandidatesSelector;
    this.allowQueuesBalanceAfterAllQueuesSatisfied =
        allowQueuesBalanceAfterAllQueuesSatisfied;
    stepFactor = Resource.newInstance(0, 0);
    // 初始化所有资源类型步长为1
    for (ResourceInformation ri : stepFactor.getResources()) {
      ri.setValue(1);
    }
  }

  /**
   * 在给定队列集合中计算未分配资源的不动点分配。
   * 当队列的所有请求都被满足后，将其从候选集合移除，剩余容量在剩余队列间重新分配。
   * 分配默认按保证容量加权，若忽略保证则均匀分配。
   *
   * @param totGuarant
   *          总保证资源
   * @param qAlloc
   *          子队列列表
   * @param unassigned
   *          待分配资源总量
   * @param ignoreGuarantee
   *          是否忽略队列保证容量，均匀分配
   */
  protected void computeFixpointAllocation(Resource totGuarant,
      Collection<TempQueuePerPartition> qAlloc, Resource unassigned,
      boolean ignoreGuarantee) {
    // 初始化每个队列理想分配：
    // 如果已使用 > 保证容量: 理想分配 = 保证容量 + 不可抢占额外资源
    // 否则: 理想分配 = 当前已使用
    // 从未分配中减去理想分配，若理想分配仍小于（已使用+待分配），则将队列加入待分配集合
    // 按缺资源程度排序，最缺资源优先
    TQComparator tqComparator = new TQComparator(rc, totGuarant);
    PriorityQueue<TempQueuePerPartition> orderedByNeed = new PriorityQueue<>(10,
        tqComparator);
    // 遍历所有队列，完成初始理想分配
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext(); ) {
      TempQueuePerPartition q = i.next();
      Resource used = q.getUsed();

      Resource initIdealAssigned;
      if (Resources.greaterThan(rc, totGuarant, used, q.getGuaranteed())) {
        initIdealAssigned = Resources.add(
            Resources.componentwiseMin(q.getGuaranteed(), q.getUsed()),
            q.untouchableExtra);
      } else{
        initIdealAssigned = Resources.clone(used);
      }

      // 执行初始分配，允许子类覆盖行为
      initIdealAssignment(totGuarant, q, initIdealAssigned);

      // 从未分配资源中减去当前队列已分配的理想资源
      Resources.subtractFrom(unassigned, q.idealAssigned);

      // 如果理想分配仍小于（已使用+待请求），说明队列还需要更多资源，加入缺资源队列优先级队列
      Resource curPlusPend = Resources.add(q.getUsed(), q.pending);
      if (Resources.lessThan(rc, totGuarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }

    // 持续分配资源直到没有需求或没有剩余资源
    while (!orderedByNeed.isEmpty() && Resources.greaterThan(rc, totGuarant,
        unassigned, Resources.none())) {
      // 根据当前活跃队列重新计算归一化保证容量
      resetCapacity(orderedByNeed, ignoreGuarantee);

      // 获取当前最缺资源的队列集合（相同缺资源程度的一起处理）
      Collection<TempQueuePerPartition> underserved = getMostUnderservedQueues(
          orderedByNeed, tqComparator);

      // 拷贝本轮未分配资源，避免计算过程中被修改
      Resource dupUnassignedForTheRound = Resources.clone(unassigned);

      // 遍历处理每个缺资源队列
      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        // 没有可用资源则提前退出
        if (!rc.isAnyMajorResourceAboveZero(unassigned)) {
          break;
        }

        TempQueuePerPartition sub = i.next();

        // 根据归一化保证容量计算当前队列可分配的资源量
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            dupUnassignedForTheRound,
            sub.normalizedGuarantee, this.stepFactor);

        // 不超过剩余未分配资源总量
        wQavail = Resources.componentwiseMin(wQavail, unassigned);

        // 将资源分配给队列，返回队列未用完的资源
        Resource wQidle = sub.offer(wQavail, rc, totGuarant,
            isReservedPreemptionCandidatesSelector,
            allowQueuesBalanceAfterAllQueuesSatisfied);
        // 计算实际分配给队列的资源量
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        // 如果分配后队列仍需要更多资源，重新放回优先级队列等待下一轮分配
        if (Resources.greaterThan(rc, totGuarant, wQdone, Resources.none())) {
          orderedByNeed.add(sub);
        }

        // 从未分配资源中减去实际分配的资源
        Resources.subtractFrom(unassigned, wQdone);

        // 确保未分配资源各维度不小于0
        unassigned = Resources.componentwiseMax(unassigned, Resources.none());
      }
    }

    // 将所有仍在优先级队列中的缺资源分区加入上下文的缺资源队列列表
    // 即使所有队列都已满足保证，也可能存在队列内部不平衡，需要全部加入以便后续处理
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      context.addPartitionToUnderServedQueues(q1.queueName, q1.partition);
    }
  }


  /**
   * 允许子类覆盖初始理想分配行为。
   *
   * @param totGuarant 总资源，用于资源计算器操作
   * @param q 待初始化的分区临时队列
   * @param initIdealAssigned 建议的初始理想分配值
   */
  protected void initIdealAssignment(Resource totGuarant,
      TempQueuePerPartition q, Resource initIdealAssigned) {
    q.idealAssigned = initIdealAssigned;
  }

  /**
   * 根据当前活跃队列重新计算归一化保证容量。
   *
   * @param queues
   *          需要考虑的队列集合
   * @param ignoreGuar
   *          是否忽略保证容量均匀分配
   */
  private void resetCapacity(Collection<TempQueuePerPartition> queues,
                             boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);
    float activeTotalAbsCap = 0.0f;
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();

    // 如果忽略保证容量，所有队列均匀分配
    if (ignoreGuar) {
      for (int i = 0; i < maxLength; i++) {
        for (TempQueuePerPartition q : queues) {
          computeNormGuarEvenly(q, queues.size(), i);
        }
      }
    } else {
      // 累加所有活跃队列的总保证容量和总绝对容量
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.getGuaranteed());
        activeTotalAbsCap += q.getAbsCapacity();
      }

      // 遍历所有资源类型，确定并应用归一化策略
      for (int i = 0; i < maxLength; i++) {
        boolean useAbsCapBasedNorm = false;
        // 如果总绝对容量为0，则均匀分配
        boolean useEvenlyDistNorm = activeTotalAbsCap == 0;

        // 第一次遍历队列，确定当前资源类型使用哪种归一化策略
        for (TempQueuePerPartition q : queues) {
          NormalizationTuple normTuple = new NormalizationTuple(
              q.getGuaranteed(), activeCap);
          long queueGuaranValue = normTuple.getNumeratorValue(i);
          long totalActiveGuaranValue = normTuple.getDenominatorValue(i);

          // 如果当前队列该资源保证值为0，但绝对容量不为0，且总保证不为0，则使用基于绝对容量的归一化
          if (queueGuaranValue == 0 && q.getAbsCapacity() != 0 && totalActiveGuaranValue != 0) {
            useAbsCapBasedNorm = true;
            break;
          }

          // 如果总保证值为0，说明所有活跃队列该资源保证容量都很小（舍入后为0），切换为均匀分配
          if (totalActiveGuaranValue == 0) {
            useEvenlyDistNorm = true;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Queue normalization strategy: " +
              "absoluteCapacityBasedNormalization(" + useAbsCapBasedNorm +
              "), evenlyDistributedNormalization(" + useEvenlyDistNorm +
              "), defaultNormalization(" + !(useAbsCapBasedNorm || useEvenlyDistNorm) + ")");
        }

        // 第二次遍历队列，应用选定的归一化策略
        for (TempQueuePerPartition q : queues) {
          if (useAbsCapBasedNorm) {
            computeNormGuarFromAbsCapacity(q, activeTotalAbsCap, i);
          } else if (useEvenlyDistNorm) {
            computeNormGuarEvenly(q, queues.size(), i);
          } else {
            computeDefaultNormGuar(q, activeCap, i);
          }
        }
      }
    }
  }

  /**
   * 基于队列绝对容量权重计算归一化保证容量。
   *
   * 示例：两个活跃队列 queueA 和 queueB，配置绝对最小容量分别为1%和3%，
   * 归一化后保证容量为：
   *   queueA = 0.01 / (0.01 + 0.03) = 0.25
   *   queueB = 0.03 / (0.01 + 0.03) = 0.75
   *
   * @param q
   *          待计算队列
   * @param activeTotalAbsCap
   *          所有活跃队列绝对容量之和
   * @param resourceTypeIdx
   *          当前处理资源类型索引
   */
  private static void computeNormGuarFromAbsCapacity(TempQueuePerPartition q,
                                                     float activeTotalAbsCap,
                                                     int resourceTypeIdx) {
    if (activeTotalAbsCap != 0) {
      q.normalizedGuarantee[resourceTypeIdx] = q.getAbsCapacity() / activeTotalAbsCap;
    }
  }

  /**
   * 按活跃队列数量均匀计算归一化保证容量。
   *
   * @param q
   *          待计算队列
   * @param numOfActiveQueues
   *          活跃队列总数
   * @param resourceTypeIdx
   *          当前处理资源类型索引
   */
  private static void computeNormGuarEvenly(TempQueuePerPartition q,
                                            int numOfActiveQueues,
                                            int resourceTypeIdx) {
    q.normalizedGuarantee[resourceTypeIdx] = 1.0f / numOfActiveQueues;
  }

  /**
   * 默认归一化保证容量计算方式。
   * 对每个资源类型，队列保证容量除以所有活跃队列总保证容量得到归一化比例。
   *
   * @param q
   *          待计算队列
   * @param activeCap
   *          所有活跃队列总保证容量
   * @param resourceTypeIdx
   *          当前处理资源类型索引
   */