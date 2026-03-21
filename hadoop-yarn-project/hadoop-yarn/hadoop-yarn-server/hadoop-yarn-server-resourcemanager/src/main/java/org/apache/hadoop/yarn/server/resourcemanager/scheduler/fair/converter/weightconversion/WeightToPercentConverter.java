// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;

/**
 * 公平调度器权重转容量调度器百分比转换器，将公平调度器队列权重转换为容量调度器的容量百分比配置
 */
public class WeightToPercentConverter
    implements CapacityConverter {

  // 常量100，保留3位小数
  private static final BigDecimal HUNDRED = new BigDecimal(100).setScale(3);
  // 常量0，保留3位小数
  private static final BigDecimal ZERO = new BigDecimal(0).setScale(3);

  @Override
  /**
   * 转换父队列下所有子队列的权重为容量百分比，并写入容量调度器配置
   * @param queue 父队列，包含待转换的子队列
   * @param csConfig 容量调度器配置对象，用于写入转换结果
   */
  public void convertWeightsForChildQueues(FSQueue queue,
      CapacitySchedulerConfiguration csConfig) {
    List<FSQueue> children = queue.getChildQueues();

    // 计算所有子队列总权重
    int totalWeight = getTotalWeight(children);
    // 计算各子队列容量百分比，返回结果和是否需要允许零容量和标记
    Pair<Map<String, BigDecimal>, Boolean> result =
        getCapacities(totalWeight, children);

    Map<String, BigDecimal> capacities = result.getLeft();
    boolean shouldAllowZeroSumCapacity = result.getRight();

    // 将转换后的容量百分比写入配置
    capacities
        .forEach((key, value) -> csConfig.setCapacity(new QueuePath(key), value.toString()));

    // 如果需要允许零容量和，配置对应属性
    if (shouldAllowZeroSumCapacity) {
      String queueName = queue.getName();
      csConfig.setAllowZeroCapacitySum(new QueuePath(queueName), true);
    }
  }

  /**
   * 根据总权重和子队列列表，计算每个子队列对应的容量百分比
   * @param totalWeight 所有子队列总权重
   * @param children 子队列列表
   * @return 左值为队列名->容量百分比映射，右值为是否需要允许零容量和
   */
  private Pair<Map<String, BigDecimal>, Boolean> getCapacities(int totalWeight,
      List<FSQueue> children) {

    if (children.size() == 0) {
      return Pair.of(new HashMap<>(), false);
    } else if (children.size() == 1) {
      // 仅一个子队列时分配100%容量
      Map<String, BigDecimal> capacity = new HashMap<>();
      String queueName = children.get(0).getName();
      capacity.put(queueName, HUNDRED);

      return Pair.of(capacity, false);
    } else {
      Map<String, BigDecimal> capacities = new HashMap<>();

      children
          .stream()
          .forEach(queue -> {
            BigDecimal pct;

            // 总权重为0时所有队列容量设为0
            if (totalWeight == 0) {
              pct = ZERO;
            } else {
              // 根据权重占比计算容量百分比，保留3位小数
              BigDecimal total = new BigDecimal(totalWeight);
              BigDecimal weight = new BigDecimal(queue.getWeight());
              pct = weight
                  .setScale(5)
                  .divide(total, RoundingMode.HALF_UP)
                  .multiply(HUNDRED)
                  .setScale(3);
            }

            capacities.put(queue.getName(), pct);
          });

      // 计算所有百分比总和
      BigDecimal totalPct = ZERO;
      for (Map.Entry<String, BigDecimal> entry : capacities.entrySet()) {
        totalPct = totalPct.add(entry.getValue());
      }

      // 若总和不等于100，修正容量误差
      boolean shouldAllowZeroSumCapacity = false;
      if (!totalPct.equals(HUNDRED)) {
        shouldAllowZeroSumCapacity = fixCapacities(capacities, totalPct);
      }

      return Pair.of(capacities, shouldAllowZeroSumCapacity);
    }
  }

  @VisibleForTesting
  /**
   * 修正四舍五入导致的容量百分比总和不等于100的误差
   * @param capacities 各队列容量百分比映射
   * @param totalPct 当前总和
   * @return 是否需要开启允许零容量和
   */
  boolean fixCapacities(Map<String, BigDecimal> capacities,
      BigDecimal totalPct) {
    boolean shouldAllowZeroSumCapacity = false;

    // 按容量从高到低排序，选择最大容量修正误差，最小化误差影响；同时避免修改零容量队列
    List<Map.Entry<String, BigDecimal>> sortedEntries = capacities
        .entrySet()
        .stream()
        .sorted(new Comparator<Map.Entry<String, BigDecimal>>() {
          @Override
          public int compare(Map.Entry<String, BigDecimal> e1,
              Map.Entry<String, BigDecimal> e2) {
            return e2.getValue().compareTo(e1.getValue());
          }
        })
        .collect(Collectors.toList());

    String highestCapacityQueue = sortedEntries.get(0).getKey();
    BigDecimal highestCapacity = sortedEntries.get(0).getValue();

    if (highestCapacity.equals(ZERO)) {
      // 所有队列都是零容量，需要开启允许零容量和
      shouldAllowZeroSumCapacity = true;
    } else {
      // 将差值补到最大容量队列上，保证总和正好为100
      BigDecimal diff = HUNDRED.subtract(totalPct);
      BigDecimal correctedHighest = highestCapacity.add(diff);
      capacities.put(highestCapacityQueue, correctedHighest);
    }

    return shouldAllowZeroSumCapacity;
  }

  /**
   * 计算所有子队列的权重总和
   * @param children 子队列列表
   * @return 总权重值
   */
  private int getTotalWeight(List<FSQueue> children) {
    double sum = children
                  .stream()
                  .mapToDouble(c -> c.getWeight())
                  .sum();
    return (int) sum;
  }
}