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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
 * DRF（主导资源公平分配）调度策略实现，通过均衡各个可调度对象的主导资源使用率来做调度决策。
 * 可调度对象的主导资源使用率是指该对象所有已使用资源类型中，使用率（已使用量/总容量）最大的那个资源的比值。
 */
@Private
@Unstable
public class DominantResourceFairnessPolicy extends SchedulingPolicy {

  public static final String NAME = "DRF";

  private static final int NUM_RESOURCES =
      ResourceUtils.getNumberOfCountableResourceTypes();
  private static final DominantResourceFairnessComparator COMPARATORN =
      new DominantResourceFairnessComparatorN();
  private static final DominantResourceFairnessComparator COMPARATOR2 =
      new DominantResourceFairnessComparator2();
  private static final DominantResourceCalculator CALCULATOR =
      new DominantResourceCalculator();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    // 优化性能：如果只有CPU和内存两种常见资源类型，使用专门优化的两资源比较器
    if (NUM_RESOURCES == 2) {
      return COMPARATOR2;
    } else {
      // 其他情况使用通用N资源比较器
      return COMPARATORN;
    }

  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return CALCULATOR;
  }

  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    // 遍历所有资源类型，分别计算公平份额
    for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
      ComputeFairShares.computeShares(schedulables, totalResources,
          info.getName());
    }
  }

  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
      Resource totalResources) {
    // 遍历所有资源类型，分别计算稳定公平份额
    for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
      ComputeFairShares.computeSteadyShares(queues, totalResources,
          info.getName());
    }
  }

  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    // 判断当前资源使用是否超过了公平份额
    return !Resources.fitsIn(usage, fairShare);
  }

  @Override
  public Resource getHeadroom(Resource queueFairShare, Resource queueUsage,
                              Resource maxAvailable) {
    // 计算队列剩余可分配内存容量
    long queueAvailableMemory =
        Math.max(queueFairShare.getMemorySize() - queueUsage.getMemorySize(), 0);
    // 计算队列剩余可分配CPU容量
    int queueAvailableCPU =
        Math.max(queueFairShare.getVirtualCores() - queueUsage
            .getVirtualCores(), 0);
    // 取队列可用容量和集群剩余可用容量的较小值作为可用资源（可分配容量）
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemorySize(), queueAvailableMemory),
        Math.min(maxAvailable.getVirtualCores(),
            queueAvailableCPU));
    return headroom;
  }

  @Override
  public void initialize(FSContext fsContext) {
    // 初始化两个比较器的上下文
    COMPARATORN.setFSContext(fsContext);
    COMPARATOR2.setFSContext(fsContext);
  }

  /**
   * DRF策略比较器抽象基类，按照DRF策略比较两个可调度对象。
   * 当两个对象都满足最小资源份额要求时，按近似公平份额比例排序；子类根据资源数量做了针对性实现。
   */
  public abstract static class DominantResourceFairnessComparator
      implements Comparator<Schedulable> {
    protected FSContext fsContext;

    public void setFSContext(FSContext fsContext) {
      this.fsContext = fsContext;
    }

    /**
     * 公平比例相同时的打破平局方法，通过提交时间和作业名称得到确定性排序，方便单元测试。
     *
     * @param s1 第一个待比较对象
     * @param s2 第二个待比较对象
     * @return &lt; 0, 0, or &gt; 0 分别表示第一个对象小于、等于、大于第二个对象
     */
    protected int compareAttributes(Schedulable s1, Schedulable s2) {
      int res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());

      if (res == 0) {
        res = s1.getName().compareTo(s2.getName());
      }

      return res;
    }
  }

  /**
   * 支持任意数量资源类型的通用DRF比较器，按照DRF策略比较两个可调度对象。
   * 当两个对象都满足最小资源份额要求时，按近似公平份额比例排序。
   */
  @VisibleForTesting
  static class DominantResourceFairnessComparatorN
      extends DominantResourceFairnessComparator {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      // 获取两个对象当前已使用资源
      Resource usage1 = s1.getResourceUsage();
      Resource usage2 = s2.getResourceUsage();
      // 获取两个对象最小资源份额
      Resource minShare1 = s1.getMinShare();
      Resource minShare2 = s2.getMinShare();
      // 获取集群总资源容量
      Resource clusterCapacity = fsContext.getClusterResource();

      // 比率数组结构：ratios[x][0]=使用率、ratios[x][1]=公平份额率、ratios[x][2]=最小份额率
      float[][] ratios1 = new float[NUM_RESOURCES][3];
      float[][] ratios2 = new float[NUM_RESOURCES][3];

      // 计算两个对象每个资源的集群使用率和近似公平份额率，并得到各自的主导资源索引
      int dominant1 = calculateClusterAndFairRatios(usage1, clusterCapacity,
          ratios1, s1.getWeight());
      int dominant2 = calculateClusterAndFairRatios(usage2, clusterCapacity,
          ratios2, s2.getWeight());

      // 判断对象是否急需资源：主导资源的已使用量小于最小资源份额时，判定为急需资源
      boolean s1Needy =
          usage1.getResources()[dominant1].getValue() <
          minShare1.getResources()[dominant1].getValue();
      boolean s2Needy =
          usage2.getResources()[dominant2].getValue() <
          minShare2.getResources()[dominant2].getValue();
      
      int res;

      if (!s2Needy && !s1Needy) {
        // 都不急需：按使用率降序排序后，比较公平份额率
        sortRatios(ratios1, ratios2);
        res = compareRatios(ratios1, ratios2, 1);
      } else if (s1Needy && !s2Needy) {
        // s1急需，优先级更高
        res = -1;
      } else if (s2Needy && !s1Needy) {
        // s2急需，优先级更高
        res = 1;
      } else { // 两者都急需
        // 计算最小份额率，按使用率降序排序后，比较最小份额率
        calculateMinShareRatios(usage1, minShare1, ratios1);
        calculateMinShareRatios(usage2, minShare2, ratios2);
        sortRatios(ratios1, ratios2);
        res = compareRatios(ratios1, ratios2, 2);
      }

      if (res == 0) {
        // 平局，用属性比较打破平局
        res = compareAttributes(s1, s2);
      }

      return res;
    }

    /**
     * 对两个比率数组按照使用率（数组第一个元素）降序排序。
     *
     * @param ratios1 第一个比率数组
     * @param ratios2 第二个比率数组
     */
    @VisibleForTesting
    void sortRatios(float[][] ratios1, float[][]ratios2) {
      // 按资源使用率降序排序
      Arrays.sort(ratios1, (float[] o1, float[] o2) ->
          (int) Math.signum(o2[0] - o1[0]));
      Arrays.sort(ratios2, (float[] o1, float[] o2) ->
          (int) Math.signum(o2[0] - o1[0]));
    }

    /**
     * 计算每个资源类型的使用率和近似公平份额率，填充到输出数组，并返回主导资源索引。
     * 使用率 = 已使用资源量 / 集群总资源量；近似公平份额率 = 使用率 / 权重。
     *
     * @param resource 待计算的已使用资源
     * @param cluster 集群总资源
     * @param ratios 输出：存储计算得到的比率数组
     * @param weight 资源权重
     * @return 主导资源索引（使用率最大的资源）
     */
    @VisibleForTesting
    int calculateClusterAndFairRatios(Resource resource, Resource cluster,
        float[][] ratios, float weight) {
      ResourceInformation[] resourceInfo = resource.getResources();
      ResourceInformation[] clusterInfo = cluster.getResources();
      int max = 0;

      // 遍历所有资源类型
      for (int i = 0; i < clusterInfo.length; i++) {
        // 计算集群使用率
        ratios[i][0] =
            resourceInfo[i].getValue() / (float) clusterInfo[i].getValue();

        // 更新最大使用率对应的资源索引，找到当前主导资源
        if (ratios[i][0] > ratios[max][0]) {
          max = i;
        }

        // 除以权重得到近似公平份额率。权重为0时会得到Infinity，该对象会自动排在后面
        ratios[i][1] = ratios[i][0] / weight;
      }

      return max;
    }
    
    /**
     * 计算每个资源类型的最小份额率（已使用量 / 最小份额量），填充到输出数组第三个位置。
     *
     * @param resource 已使用资源
     * @param minShare 最小资源份额
     * @param ratios 输出：存储计算得到的比率数组
     */
    @VisibleForTesting
    void calculateMinShareRatios(Resource resource, Resource minShare,
        float[][] ratios) {
      ResourceInformation[] resourceInfo = resource.getResources();
      ResourceInformation[] minShareInfo = minShare.getResources();

      for (int i = 0; i < minShareInfo.length; i++) {
        ratios[i][2] =
            resourceInfo[i].getValue() / (float) minShareInfo[i].getValue();
      }
    }

    /**
     * 按顺序比较两个比率数组对应位置的指定索引比率，返回比较结果。
     * 数组已按使用率降序排序，从第一个元素开始比较，第一个不相等的结果就是最终比较结果。
     *
     * @param ratios1 第一个比率数组
     * @param ratios2 第二个比率数组
     * @param index 内部数组要比较的索引：0=使用率、1=公平份额率、2=最小份额率
     * @return -1, 0, 或 1 分别表示第一个数组小于、等于、大于第二个数组
     */
    @VisibleForTesting
    int compareRatios(float[][] ratios1, float[][] ratios2, int index) {
      int ret = 0;

      // 按顺序逐个比较，第一个不相等就是结果
      for (int i = 0; i < ratios1.length; i++) {
        ret = (int) Math.signum(ratios1[i][index] - ratios2[i][index]);

        if (ret != 0) {
          break;
        }
      }

      return ret;
    }
  }

  /**
   * 仅针对CPU和内存两种资源优化的DRF比较器，针对只有CPU和内存的场景做性能优化。
   * 当两个对象都满足最小资源份额要求时，按近似公平份额比例排序。
   */
  @VisibleForTesting
  static class DominantResourceFairnessComparator2
      extends DominantResourceFairnessComparator {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      // 获取两个对象资源使用信息
      ResourceInformation[] resourceInfo1 =
          s1.getResourceUsage().getResources();
      ResourceInformation[] resourceInfo2 =
          s2.getResourceUsage().getResources();
      // 获取两个对象最小份额信息
      ResourceInformation[] minShareInfo1 = s1.getMinShare().getResources();
      ResourceInformation[] minShareInfo2 = s2.getMinShare().getResources();
      // 获取集群总资源信息
      ResourceInformation[] clusterInfo =
          fsContext.getClusterResource().getResources();
      // 存储公平份额率
      double[] shares1 = new double[2];
      double[] shares2 = new double[2];

      // 计算近似公平份额率，得到各自主导资源索引
      int dominant1 = calculateClusterAndFairRatios(resourceInfo1,
          s1.getWeight(), clusterInfo, shares1);
      int dominant2 = calculateClusterAndFairRatios(resourceInfo2,
          s2.getWeight(), clusterInfo, shares2);

      // 判断是否急需资源：主导资源使用量小于最小份额时判定为急需
      boolean s1Needy = resourceInfo1[dominant1].getValue() <
          minShareInfo1[dominant1].getValue();
      boolean s2Needy = resourceInfo2[dominant2].getValue() <
          minShareInfo2[dominant2].getValue();

      int res;

      if (!s2Needy && !s1Needy) {
        // 都不急需：先比较主导资源的公平份额率
        res = (int) Math.signum(shares1[dominant1] - shares2[dominant2]);

        if (res == 0) {
          // 相等，再比较非主导资源的公平份额率
          res = (int) Math.signum(shares1[1 - dominant1] -
              shares2[1 - dominant2]);
        }
      } else if (s1Needy && !s2Needy) {
        // s1急需，优先级更高
        res = -1;
      } else if (s2Needy && !s1Needy) {
        // s2急需，优先级更高
        res = 1;
      } else {
        // 都急需：计算最小份额率
        double[] minShares1 =
            calculateMinShareRatios(resourceInfo1, minShareInfo1);
        double[] minShares2 =
            calculateMinShareRatios(resourceInfo2, minShareInfo2);

        // 先比较主导资源的最小份额率
        res = (int) Math.signum(minShares1[dominant1] - minShares2[dominant2]);

        if (res == 0) {
          // 相等，再比较非主导资源的最小份额率
          res = (int) Math.signum(minShares1[1 - dominant1] -
              minShares2[1 - dominant2]);
        }
      }

      if (res == 0) {
        // 平局，用属性比较打破平局
        res = compareAttributes(s1, s2);
      }

      return res;
    }

    /**
     * 针对仅CPU和内存两种资源的场景，计算每个资源的近似公平份额率，返回主导资源索引。
     *
     * @param resourceInfo 已使用资源信息数组
     * @param weight 资源权重
     * @param clusterInfo 集群总资源信息数组
     * @param shares 输出：存储计算得到的公平份额率
     * @return 主导资源索引（使用率最大的资源）
     */
    @VisibleForTesting
    int calculateClusterAndFairRatios(ResourceInformation