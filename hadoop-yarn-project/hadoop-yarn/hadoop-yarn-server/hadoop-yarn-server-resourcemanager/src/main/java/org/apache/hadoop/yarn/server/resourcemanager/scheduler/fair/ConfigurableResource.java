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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.Arrays;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
 * 公平调度器中可配置的资源容器，支持按绝对数值或集群百分比两种方式配置队列资源
 * 用于队列最大资源、最小资源等可配置资源项的存储和计算
 */
@Private
@Unstable
public class ConfigurableResource {
  private final Resource resource;
  private final double[] percentages;

  /**
   * 默认构造函数，所有资源类型默认使用100%集群资源
   */
  ConfigurableResource() {
    this(getOneHundredPercentArray());
  }

  /**
   * 按百分比构造可配置资源对象
   * @param percentages 各资源类型占集群资源的百分比数组
   */
  ConfigurableResource(double[] percentages) {
    this.percentages = percentages.clone();
    this.resource = null;
  }

  /**
   * 创建所有资源都使用相同绝对数值的可配置资源实例
   * @param value 所有资源统一使用的绝对数值
   */
  ConfigurableResource(long value) {
    this(ResourceUtils.createResourceWithSameValue(value));
  }

  /**
   * 按绝对资源值构造可配置资源对象
   * @param resource 绝对资源值
   */
  public ConfigurableResource(Resource resource) {
    this.percentages = null;
    this.resource = resource;
  }

  /**
   * 生成所有资源类型都是100%占比的百分比数组
   * @return 填充完1.0的百分比数组
   */
  private static double[] getOneHundredPercentArray() {
    // 根据当前可计数资源类型总数创建数组
    double[] resourcePercentages =
        new double[ResourceUtils.getNumberOfCountableResourceTypes()];
    // 所有位置填充1.0（即100%）
    Arrays.fill(resourcePercentages, 1.0);

    return resourcePercentages;
  }

  /**
   * 根据集群总资源计算最终实际资源值
   * 如果配置了百分比则按比例计算，否则返回配置的绝对资源值
   *
   * @param clusterResource 集群总资源
   * @return 计算得到的最终资源值
   */
  public Resource getResource(Resource clusterResource) {
    // 同时存在百分比配置和集群资源，按比例计算
    if (percentages != null && clusterResource != null) {
      // 计算内存资源，内存百分比在数组索引0位置
      long memory = (long) (clusterResource.getMemorySize() * percentages[0]);
      // 计算CPU核心资源，vcore百分比在数组索引1位置
      int vcore = (int) (clusterResource.getVirtualCores() * percentages[1]);
      // 创建基础资源对象
      Resource res = Resource.newInstance(memory, vcore);
      // 获取集群其他资源信息
      ResourceInformation[] clusterInfo = clusterResource.getResources();

      // 遍历索引从2开始的其他自定义资源
      for (int i = 2; i < clusterInfo.length; i++) {
        // 按百分比计算自定义资源值并设置
        res.setResourceValue(i,
            (long)(clusterInfo[i].getValue() * percentages[i]));
      }

      return res;
    } else {
      // 无百分比配置，直接返回绝对资源值
      return resource;
    }
  }

  /**
   * 获取配置的绝对资源值
   *
   * @return 绝对资源值，如果是百分比配置则返回null
   */
  public Resource getResource() {
    return resource;
  }

  /**
   * 设置指定资源的绝对数值，如果当前对象是百分比配置则此方法无效果
   *
   * @param name 资源名称
   * @param value 要设置的绝对数值
   */
  void setValue(String name, long value) {
    if (resource != null) {
      resource.setResourceValue(name, value);
    }
  }

  /**
   * 设置指定资源的占比，如果当前对象是绝对数值配置则此方法无效果
   *
   * @param name 资源名称
   * @param value 要设置的百分比（0~1）
   */
  void setPercentage(String name, double value) {
    if (percentages != null) {
      // 获取资源类型对应索引
      Integer index = ResourceUtils.getResourceTypeIndex().get(name);

      if (index != null) {
        // 更新对应位置的百分比
        percentages[index] = value;
      } else {
        // 资源不存在抛出异常
        throw new ResourceNotFoundException("The requested resource, \""
            + name + "\", could not be found.");
      }
    }
  }

  /**
   * 获取所有资源的百分比数组副本
   * @return 百分比数组副本，如果是绝对配置返回null
   */
  public double[] getPercentages() {
    return percentages == null ? null :
      Arrays.copyOf(percentages, percentages.length);
  }
}