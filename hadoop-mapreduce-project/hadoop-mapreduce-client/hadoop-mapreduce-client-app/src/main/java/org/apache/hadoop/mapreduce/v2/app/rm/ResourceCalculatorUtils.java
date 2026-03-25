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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

import java.util.EnumSet;

/**
 * MapReduce应用向RM申请资源的资源计算工具类
 * 提供基于内存和CPU资源计算可分配容器数量、向上取整除法等通用能力
 */
public class ResourceCalculatorUtils {
  /**
   * 对两个整数做除法，向上取整结果
   * @param a 被除数
   * @param b 除数
   * @return 向上取整后的除法结果，除数为0时返回0
   */
  public static int divideAndCeil(long a, long b) {
    if (b == 0) {
      return 0;
    }
    return (int) ((a + (b - 1)) / b);
  }

  /**
   * 根据可用总资源和单个容器所需资源，计算可分配的容器数量
   * @param available 集群节点可用总资源
   * @param required 单个容器所需资源
   * @param resourceTypes 调度器支持的资源类型集合
   * @return 可分配的容器数量
   */
  public static int computeAvailableContainers(Resource available,
      Resource required, EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {
      return Math.min(
        calculateRatioOrMaxValue(available.getMemorySize(), required.getMemorySize()),
        calculateRatioOrMaxValue(available.getVirtualCores(), required
            .getVirtualCores()));
    }
    return calculateRatioOrMaxValue(
      available.getMemorySize(), required.getMemorySize());
  }

  /**
   * 根据总需求资源和单个容器可分配资源，计算需要申请的容器数量（向上取整）
   * @param required 总需求资源
   * @param factor 单个容器可分配资源上限
   * @param resourceTypes 调度器支持的资源类型集合
   * @return 需要申请的容器数量，取内存和CPU计算结果的较大值
   */
  public static int divideAndCeilContainers(Resource required, Resource factor,
      EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {
      return Math.max(divideAndCeil(required.getMemorySize(), factor.getMemorySize()),
        divideAndCeil(required.getVirtualCores(), factor.getVirtualCores()));
    }
    return divideAndCeil(required.getMemorySize(), factor.getMemorySize());
  }

  /**
   * 计算分子分母的整数比，分母为0时返回整数最大值表示无限制
   * @param numerator 分子（可用资源总量）
   * @param denominator 分母（单个容器所需资源）
   * @return 可分配容器数量，分母为0时返回Integer.MAX_VALUE
   */
  private static int calculateRatioOrMaxValue(long numerator, long denominator) {
    if (denominator == 0) {
      return Integer.MAX_VALUE;
    }
    return (int) (numerator / denominator);
  }
}