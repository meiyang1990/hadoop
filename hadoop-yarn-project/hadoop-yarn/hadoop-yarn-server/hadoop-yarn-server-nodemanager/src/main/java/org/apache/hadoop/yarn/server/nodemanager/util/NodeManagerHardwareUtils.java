// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.Map;

/**
 * NodeManager硬件信息工具类，用于获取节点CPU、内存等硬件特征信息，
 * 结合配置计算可分配给YARN容器的资源量。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NodeManagerHardwareUtils {

  private static final Logger LOG =
       LoggerFactory.getLogger(NodeManagerHardwareUtils.class);

  /**
   * 检查是否开启硬件能力自动检测。
   * @param conf 配置对象
   * @return 是否开启自动检测
   */
  private static boolean isHardwareDetectionEnabled(Configuration conf) {
    return conf.getBoolean(
        YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        YarnConfiguration.DEFAULT_NM_ENABLE_HARDWARE_CAPABILITY_DETECTION);
  }

  /**
   * 获取节点CPU总数，根据配置决定是否将逻辑处理器（超线程）计入核数。
   * @param conf 配置对象
   * @return 节点CPU总数
   */
  public static int getNodeCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
  }

  /**
   * 获取节点CPU总数，根据配置决定是否将逻辑处理器（超线程）计入核数。
   * @param plugin 资源计算器插件
   * @param conf 配置对象
   * @return 节点CPU总数
   */
  public static int getNodeCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = plugin.getNumProcessors();
    boolean countLogicalCores =
        conf.getBoolean(YarnConfiguration.NM_COUNT_LOGICAL_PROCESSORS_AS_CORES,
          YarnConfiguration.DEFAULT_NM_COUNT_LOGICAL_PROCESSORS_AS_CORES);
    if (!countLogicalCores) {
      numProcessors = plugin.getNumCores();
    }
    return numProcessors;
  }

  /**
   * 计算可分配给YARN容器的CPU数量，根据配置的CPU占比计算。
   * @param conf 配置对象
   * @return 可分配给容器的CPU数量
   */
  public static float getContainersCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
  }

  /**
   * 计算可分配给YARN容器的CPU数量，根据配置的CPU占比计算。
   * @param plugin 资源计算器插件
   * @param conf 配置对象
   * @return 可分配给容器的CPU数量
   */
  public static float getContainersCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = getNodeCPUs(plugin, conf);
    int nodeCpuPercentage = getNodeCpuPercentage(conf);

    return (nodeCpuPercentage * numProcessors) / 100.0f;
  }

  /**
   * 获取配置的节点CPU可分配给YARN的百分比。
   * @param conf 配置对象
   * @return 百分比(0 < 值 <= 100)
   */
  public static int getNodeCpuPercentage(Configuration conf) {
    int nodeCpuPercentage =
        Math.min(conf.getInt(
          YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
          YarnConfiguration.DEFAULT_NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT),
          100);
    nodeCpuPercentage = Math.max(0, nodeCpuPercentage);

    if (nodeCpuPercentage == 0) {
      String message =
          "Illegal value for "
              + YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
              + ". Value cannot be less than or equal to 0.";
      throw new IllegalArgumentException(message);
    }
    return nodeCpuPercentage;
  }

  /**
   * 获取配置文件中指定的vcore数量，处理默认值。
   * @param conf 配置对象
   * @return 配置的vcore数量
   */
  private static int getConfiguredVCores(Configuration conf) {
    int cores = conf.getInt(YarnConfiguration.NM_VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);
    if (cores == -1) {
      cores = YarnConfiguration.DEFAULT_NM_VCORES;
    }
    return cores;
  }

  /**
   * 获取可分配给YARN容器的vcore总数。
   * 若配置中指定了数值则直接返回，否则根据硬件信息自动计算。
   * @param conf NodeManager配置对象
   * @return 可分配的vcore总数
   */
  public static int getVCores(Configuration conf) {
    if (!isHardwareDetectionEnabled(conf)) {
      return getConfiguredVCores(conf);
    }
    // 获取资源计算器插件，判断当前系统是否支持硬件检测
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    if (plugin == null) {
      return getConfiguredVCores(conf);
    }
    return getVCoresInternal(plugin, conf);
  }

  /**
   * 获取可分配给YARN容器的vcore总数。
   * 若配置中指定了数值则直接返回，否则根据硬件信息自动计算。
   * @param plugin 资源计算器插件
   * @param conf NodeManager配置对象
   * @return 可分配的vcore总数
   */
  public static int getVCores(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    if (!isHardwareDetectionEnabled(conf) || plugin == null) {
      return getConfiguredVCores(conf);
    }
    return getVCoresInternal(plugin, conf);
  }

  /**
   * 内部方法：根据物理CPU数和配置的乘数计算vcore总数。
   * @param plugin 资源计算器插件
   * @param conf 配置对象
   * @return 计算得到的vcore总数
   */
  private static int getVCoresInternal(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    String message;
    int cores = conf.getInt(YarnConfiguration.NM_VCORES, -1);
    // 配置未指定vcore数，自动计算
    if (cores == -1) {
      float physicalCores =
          NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
      float multiplier =
          conf.getFloat(YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER,
              YarnConfiguration.DEFAULT_NM_PCORES_VCORES_MULTIPLIER);
      if (multiplier > 0) {
        float tmp = physicalCores * multiplier;
        if (tmp > 0 && tmp < 1) {
          // 单核节点计算结果不足1时，至少分配1个vcore
          cores = 1;
        } else {
          cores = Math.round(tmp);
        }
      } else {
        message = "Illegal value for "
            + YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER
            + ". Value must be greater than 0.";
        throw new IllegalArgumentException(message);
      }
    }
    if(cores <= 0) {
      message = "Illegal value for " + YarnConfiguration.NM_VCORES
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }

    return cores;
  }

  /**
   * 获取配置文件中指定的容器可用内存（单位MB），处理默认值。
   * @param conf 配置对象
   * @return 配置的内存大小(MB)
   */
  private static long getConfiguredMemoryMB(Configuration conf) {
    long memoryMb = conf.getLong(YarnConfiguration.NM_PMEM_MB,
        YarnConfiguration.DEFAULT_NM_PMEM_MB);
    if (memoryMb == -1) {
      memoryMb = YarnConfiguration.DEFAULT_NM_PMEM_MB;
    }
    return memoryMb;
  }

  /**
   * 获取可分配给YARN容器的内存总数（单位MB）。
   * 若配置中指定了数值则直接返回，否则根据硬件信息自动计算。
   * @param conf NodeManager配置对象
   * @return 可分配的内存大小(MB)
   */
  public static long getContainerMemoryMB(Configuration conf) {
    if (!isHardwareDetectionEnabled(conf)) {
      return getConfiguredMemoryMB(conf);
    }
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    if (plugin == null) {
      return getConfiguredMemoryMB(conf);
    }
    return getContainerMemoryMBInternal(plugin, conf);
  }

  /**
   * 获取可分配给YARN容器的内存总数（单位MB）。
   * 若配置中指定了数值则直接返回，否则根据硬件信息自动计算。
   * @param plugin 资源计算器插件
   * @param conf NodeManager配置对象
   * @return 可分配的内存大小(MB)
   */
  public static long getContainerMemoryMB(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    if (!isHardwareDetectionEnabled(conf) || plugin == null) {
      return getConfiguredMemoryMB(conf);
    }
    return getContainerMemoryMBInternal(plugin, conf);
  }

  /**
   * 内部方法：根据节点总内存和预留内存计算可分配给容器的内存。
   * @param plugin 资源计算器插件
   * @param conf 配置对象
   * @return 计算得到的可分配内存(MB)
   */
  private static long getContainerMemoryMBInternal(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    long memoryMb = conf.getInt(YarnConfiguration.NM_PMEM_MB, -1);
    // 配置未指定内存大小，自动计算
    if (memoryMb == -1) {
      // 获取节点总物理内存，转换为MB单位
      long physicalMemoryMB = (plugin.getPhysicalMemorySize() / (1024 * 1024));
      // 获取当前NM JVM的最大堆内存，转换为MB单位
      long hadoopHeapSizeMB = (Runtime.getRuntime().maxMemory()
          / (1024 * 1024));
      // 默认算法：80% * (总内存 - 预留2倍JVM内存，给DataNode和NM各留一份)
      long containerPhysicalMemoryMB = (long) (0.8f
          * (physicalMemoryMB - (2 * hadoopHeapSizeMB)));
      // 若配置了系统预留内存，则使用配置值替换默认计算
      long reservedMemoryMB = conf
          .getInt(YarnConfiguration.NM_SYSTEM_RESERVED_PMEM_MB, -1);
      if (reservedMemoryMB != -1) {
        containerPhysicalMemoryMB = physicalMemoryMB - reservedMemoryMB;
      }
      if (containerPhysicalMemoryMB <= 0) {
        LOG.error("Calculated memory for YARN containers is too low."
            + " Node memory is " + physicalMemoryMB
            + " MB, system reserved memory is " + reservedMemoryMB + " MB.");
      }
      // 保证结果不小于0
      containerPhysicalMemoryMB = Math.max(containerPhysicalMemoryMB, 0);
      memoryMb = containerPhysicalMemoryMB;
    }
    if(memoryMb <= 0) {
      String message = "Illegal value for " + YarnConfiguration.NM_PMEM_MB
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }
    return memoryMb;
  }

  /**
   * 获取节点完整资源信息，合并自定义资源和自动计算的内存/vcore。
   * @param configuration 配置对象
   * @return 节点总资源信息
   */
  public static Resource getNodeResources(Configuration configuration) {
    Configuration conf = new Configuration(configuration);
    String memory = ResourceInformation.MEMORY_MB.getName();
    String vcores = ResourceInformation.VCORES.getName();

    Resource ret = Resource.newInstance(0, 0);
    // 从配置中加载节点资源信息
    Map<String, ResourceInformation> resourceInformation =
        ResourceUtils.getNodeResourceInformation(conf);
    // 将所有配置的资源添加到返回结果中
    for (Map.Entry<String, ResourceInformation> entry : resourceInformation
        .entrySet()) {
      ret.setResourceInformation(entry.getKey(), entry.getValue());
      LOG.debug("Setting key {} to {}", entry.getKey(), entry.getValue());
    }
    // 处理内存资源：若配置中内存值为0，自动计算
    if (resourceInformation.containsKey(memory)) {
      Long value = resourceInformation.get(memory).getValue();
      if (value > Integer.MAX_VALUE) {
        throw new YarnRuntimeException("Value '" + value
            + "' for resource memory is more than the maximum for an integer.");
      }
      ResourceInformation memResInfo = resourceInformation.get(memory);
      if(memResInfo.getValue() == 0) {
        ret.setMemorySize(getContainerMemoryMB(conf));
        LOG.debug("Set memory to {}", ret.getMemorySize());
      }
    }
    // 处理vcore资源：若配置中vcore值为0，自动计算
    if (resourceInformation.containsKey(vcores)) {
      Long value = resourceInformation.get(vcores).getValue();
      if (value > Integer.MAX_VALUE) {
        throw new YarnRuntimeException("Value '" + value
            + "' for resource vcores is more than the maximum for an integer.");
      }
      ResourceInformation vcoresResInfo = resourceInformation.get(vcores);
      if(vcoresResInfo.getValue() == 0) {
        ret.setVirtualCores(getVCores(conf));
        LOG.debug("Set vcores to {}", ret.getVirtualCores());
      }
    }
    LOG.debug("Node resource information map is {}", ret);
    return ret;
  }
}