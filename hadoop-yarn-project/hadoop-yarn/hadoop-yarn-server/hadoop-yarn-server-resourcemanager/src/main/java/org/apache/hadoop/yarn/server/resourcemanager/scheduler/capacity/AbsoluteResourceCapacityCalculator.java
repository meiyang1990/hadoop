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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType.BRANCH_DOWNSCALED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ResourceCalculationDriver.MB_UNIT;

/**
 * 绝对资源容量计算器，用于容量调度器中按绝对资源值计算队列容量
 * 负责处理子队列使用绝对资源配置时的容量归一化计算
 */
public class AbsoluteResourceCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    setNormalizedResourceRatio(resourceCalculationDriver);
  }

  @Override
  public double calculateMinimumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    String resourceName = context.getResourceName();
    double normalizedRatio = resourceCalculationDriver.getNormalizedResourceRatios().getOrDefault(
        label, ResourceVector.of(1)).getValue(resourceName);
    double remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);

    return normalizedRatio * remainingResourceRatio * context.getCurrentMinimumCapacityEntry(
        label).getResourceValue();
  }

  @Override
  public double calculateMaximumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    return context.getCurrentMaximumCapacityEntry(label).getResourceValue();
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    CapacitySchedulerQueueCapacityHandler.setQueueCapacities(
        resourceCalculationDriver.getUpdateContext()
            .getUpdatedClusterResource(label), queue, label);
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.ABSOLUTE;
  }

  /**
   * 计算父队列下所有使用绝对容量的子队列的归一化资源比例
   * 若父队列有效资源小于子队列配置资源总和，会按比例缩小所有子队列资源
   *
   * @param calculationDriver 包含当前待计算父队列的计算驱动
   */
  public static void setNormalizedResourceRatio(ResourceCalculationDriver calculationDriver) {
    CSQueue queue = calculationDriver.getQueue();

    for (String label : queue.getConfiguredNodeLabels()) {
      // 托管父队列在资源超配时会自动分配零容量，无需对子队列进行降容
      if (queue instanceof ManagedParentQueue) {
        return;
      }

      for (String resourceName : queue.getConfiguredCapacityVector(label).getResourceNames()) {
        long childrenConfiguredResource = 0;
        long effectiveMinResource = queue.getQueueResourceQuotas().getEffectiveMinResource(
            label).getResourceValue(resourceName);

        // 累加所有直接子队列配置的绝对最小资源总和
        for (CSQueue childQueue : queue.getChildQueues()) {
          if (!childQueue.getConfiguredNodeLabels().contains(label)) {
            continue;
          }
          QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
          if (capacityVector.isResourceOfType(resourceName, ResourceUnitCapacityType.ABSOLUTE)) {
            childrenConfiguredResource += capacityVector.getResource(resourceName)
                .getResourceValue();
          }
        }
        // 没有子队列使用绝对容量类型，无需归一化计算
        if (childrenConfiguredResource == 0) {
          continue;
        }
        // 计算缩容比例分子：集群资源充足时和配置总和一致
        float numeratorForMinRatio = childrenConfiguredResource;
        if (effectiveMinResource < childrenConfiguredResource) {
          numeratorForMinRatio = queue.getQueueResourceQuotas().getEffectiveMinResource(label)
              .getResourceValue(resourceName);
          // 添加队列分支降容警告
          calculationDriver.getUpdateContext().addUpdateWarning(BRANCH_DOWNSCALED.ofQueue(
              queue.getQueuePath()));
        }

        // 单位转换：将子队列配置资源转换为集群资源使用的单位
        String unit = resourceName.equals(MEMORY_URI) ? MB_UNIT : "";
        long convertedValue = UnitsConversionUtil.convert(unit, calculationDriver.getUpdateContext()
            .getUpdatedClusterResource(label).getResourceInformation(resourceName).getUnits(),
            childrenConfiguredResource);

        if (convertedValue != 0) {
          Map<String, ResourceVector> normalizedResourceRatios =
              calculationDriver.getNormalizedResourceRatios();
          normalizedResourceRatios.putIfAbsent(label, ResourceVector.newInstance());
          // 存储当前资源的归一化比例
          normalizedResourceRatios.get(label).setValue(resourceName, numeratorForMinRatio /
              convertedValue);
        }
      }
    }
  }
}