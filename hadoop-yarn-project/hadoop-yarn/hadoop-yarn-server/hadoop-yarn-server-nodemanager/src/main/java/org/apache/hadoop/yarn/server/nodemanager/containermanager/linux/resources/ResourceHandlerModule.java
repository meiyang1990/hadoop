// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceHandlerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Linux节点容器资源处理模块工厂，根据配置创建并提供CPU、内存、网络、磁盘等各类资源处理器实例，组装成资源处理链。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerModule {
  static final Logger LOG =
       LoggerFactory.getLogger(ResourceHandlerModule.class);
  // 是否启用cgroups v2
  private static boolean cgroupsV2Enabled;
  // 全局单例资源处理链
  private static volatile ResourceHandlerChain resourceHandlerChain;

  /**
   * This specific implementation might provide resource management as well
   * as resource metrics functionality. We need to ensure that the same
   * instance is used for both.
   */
  // cgroups v1处理器单例
  private static volatile CGroupsHandler cGroupV1Handler;
  // cgroups v2处理器单例
  private static volatile CGroupsHandler cGroupV2Handler;
  // 流量控制带宽处理器单例
  private static volatile TrafficControlBandwidthHandlerImpl
      trafficControlBandwidthHandler;
  // 网络包标记处理器单例
  private static volatile NetworkPacketTaggingHandlerImpl
      networkPacketTaggingHandlerImpl;
  // 磁盘IO资源处理器单例
  private static volatile CGroupsBlkioResourceHandlerImpl
      cGroupsBlkioResourceHandler;
  // 内存资源处理器单例
  private static volatile MemoryResourceHandler
      cGroupsMemoryResourceHandler;
  // CPU资源处理器单例
  private static volatile CpuResourceHandler
      cGroupsCpuResourceHandler;

  /**
   * 根据是否启用cgroups v2以及控制器挂载情况，初始化对应版本的cgroups处理器。
   * @param conf YARN配置
   * @param controller 需要初始化的cgroups控制器
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private static void initializeCGroupHandlers(Configuration conf,
                                               CGroupsHandler.CGroupController controller)
      throws ResourceHandlerException {
    if (cgroupsV2Enabled) {
      // 优先初始化cgroups v2
      initializeCGroupV2Handler(conf);
      if (!isMountedInCGroupsV2(controller)) {
        // 如果目标控制器未在v2中挂载，回退到v1
        LOG.info("Cgroup v2 is enabled but {} is not mounted in cgroups v2, falling back to v1",
            controller);
        initializeCGroupV1Handler(conf);
      }
    } else {
      // 直接初始化v1
      initializeCGroupV1Handler(conf);
    }
  }

  /**
   * 双重检查锁定初始化cgroups v1处理器单例。
   * @param conf YARN配置
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private static void initializeCGroupV1Handler(Configuration conf)
      throws ResourceHandlerException {
    if (cGroupV1Handler == null) {
      synchronized (CGroupsHandler.class) {
        if (cGroupV1Handler == null) {
          // 创建cgroups v1处理器实例，使用特权操作执行器
          cGroupV1Handler = new CGroupsHandlerImpl(
              conf, PrivilegedOperationExecutor.getInstance(conf));
          LOG.debug("Value of CGroupsV1Handler is: {}", cGroupV1Handler);
        }
      }
    }
  }

  /**
   * 双重检查锁定初始化cgroups v2处理器单例。
   * @param conf YARN配置
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private static void initializeCGroupV2Handler(Configuration conf)
      throws ResourceHandlerException {
    if (cGroupV2Handler == null) {
      synchronized (CGroupsHandler.class) {
        if (cGroupV2Handler == null) {
          // 创建cgroups v2处理器实例，使用特权操作执行器
          cGroupV2Handler = new CGroupsV2HandlerImpl(
              conf, PrivilegedOperationExecutor.getInstance(conf));
          LOG.debug("Value of CGroupsV2Handler is: {}", cGroupV2Handler);
        }
      }
    }
  }

  /**
   * 检查指定控制器是否已在cgroups v1中挂载。
   * @param controller cgroups控制器
   * @return true表示已挂载
   */
  private static boolean isMountedInCGroupsV1(CGroupsHandler.CGroupController controller) {
    return (cGroupV1Handler != null && cGroupV1Handler.getControllerPath(controller) != null);
  }

  /**
   * 检查指定控制器是否已在cgroups v2中挂载。
   * @param controller cgroups控制器
   * @return true表示已挂载
   */
  private static boolean isMountedInCGroupsV2(CGroupsHandler.CGroupController controller) {
    return (cGroupV2Handler != null && cGroupV2Handler.getControllerPath(controller) != null);
  }

  /**
   * 返回已初始化的cgroups处理器（返回v1处理器），只有当至少一个cgroups资源处理器被启用并初始化后才非空。
   */

  public static CGroupsHandler getCGroupsHandler() {
    return cGroupV1Handler;
  }

  /**
   * 获取cgroups相对根路径，去除末尾斜杠。
   * @return 相对根路径，处理器未初始化或路径为空则返回null
   */
  public static String getCgroupsRelativeRoot() {
    if (getCGroupsHandler() == null) {
      return null;
    }
    String cGroupPath = getCGroupsHandler().getRelativePathForCGroup("");
    if (cGroupPath == null || cGroupPath.isEmpty()) {
      return null;
    }
    // 去除末尾斜杠
    return cGroupPath.replaceAll("/$", "");
  }

  public static NetworkPacketTaggingHandlerImpl
      getNetworkResourceHandler() {
    return networkPacketTaggingHandlerImpl;
  }

  public static DiskResourceHandler
      getDiskResourceHandler() {
    return cGroupsBlkioResourceHandler;
  }

  public static MemoryResourceHandler
      getMemoryResourceHandler() {
    return cGroupsMemoryResourceHandler;
  }

  public static CpuResourceHandler
      getCpuResourceHandler() {
    return cGroupsCpuResourceHandler;
  }

  /**
   * 初始化CPU资源处理器，根据配置和cgroups版本创建对应实现。
   * @param conf YARN配置
   * @return 初始化好的CPU处理器，未启用则返回null
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private static CpuResourceHandler initCGroupsCpuResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    boolean cgroupsCpuEnabled =
        conf.getBoolean(YarnConfiguration.NM_CPU_RESOURCE_ENABLED,
            YarnConfiguration.DEFAULT_NM_CPU_RESOURCE_ENABLED);
    boolean cgroupsLCEResourcesHandlerEnabled =
        conf.getClass(YarnConfiguration.NM_LINUX_CONTAINER_RESOURCES_HANDLER,
            DefaultLCEResourcesHandler.class)
            .equals(CgroupsLCEResourcesHandler.class);
    // CPU资源启用或LCE使用cgroups处理器时才初始化
    if (cgroupsCpuEnabled || cgroupsLCEResourcesHandlerEnabled) {
      if (cGroupsCpuResourceHandler == null) {
        synchronized (CpuResourceHandler.class) {
          if (cGroupsCpuResourceHandler == null) {
            LOG.debug("Creating new cgroups cpu handler");

            initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.CPU);
            // 根据CPU控制器挂载版本选择对应处理器实现
            if (isMountedInCGroupsV2(CGroupsHandler.CGroupController.CPU)) {
              cGroupsCpuResourceHandler = new CGroupsV2CpuResourceHandlerImpl(cGroupV2Handler);
            } else {
              cGroupsCpuResourceHandler = new CGroupsCpuResourceHandlerImpl(cGroupV1Handler);
            }
            return cGroupsCpuResourceHandler;
          }
        }
      }
    }
    return null;
  }

  /**
   * 双重检查锁定初始化流量控制带宽处理器单例。
   * @param conf YARN配置
   * @return 初始化好的处理器，未启用则返回null
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private static TrafficControlBandwidthHandlerImpl
      getTrafficControlBandwidthHandler(Configuration conf)
        throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_NETWORK_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_ENABLED)) {
      if (trafficControlBandwidthHandler == null) {
        synchronized (OutboundBandwidthResourceHandler.class) {
          if (trafficControlBandwidthHandler == null) {
            LOG.info("Creating new traffic control bandwidth handler.");

            initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.NET_CLS);
            trafficControlBandwidthHandler = new
                TrafficControlBandwidthHandlerImpl(PrivilegedOperationExecutor
                .getInstance(conf), cGroupV1Handler,
                new TrafficController(conf, PrivilegedOperationExecutor
                    .getInstance(conf)));
          }
        }
      }

      return trafficControlBandwidthHandler;
    } else {
      return null;
    }
  }

  /**
   * 根据配置选择并初始化网络资源处理器。
   * @param conf YARN配置
   * @return 初始化好的网络处理器，未启用则返回null
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  public static ResourceHandler initNetworkResourceHandler(Configuration conf)
        throws ResourceHandlerException {
    boolean useNetworkTagHandler = conf.getBoolean(
        YarnConfiguration.NM_NETWORK_TAG_HANDLER_ENABLED,
        YarnConfiguration.DEFAULT_NM_NETWORK_TAG_HANDLER_ENABLED);
    if (useNetworkTagHandler) {
      LOG.info("Using network-tagging-handler.");
      return getNetworkTaggingHandler(conf);
    } else {
      LOG.info("Using traffic control bandwidth handler");
      return getTrafficControlBandwidthHandler(conf);
    }
  }

  /**
   * 双重检查锁定初始化网络包标记处理器单例。
   * @param conf YARN配置
   * @return 初始化好的处理器
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  public static ResourceHandler getNetworkTaggingHandler(Configuration conf)
      throws ResourceHandlerException {
    if (networkPacketTaggingHandlerImpl == null) {
      synchronized (OutboundBandwidthResourceHandler.class) {
        if (networkPacketTaggingHandlerImpl == null) {
          LOG.info("Creating new network-tagging-handler.");

          initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.NET_CLS);
          networkPacketTaggingHandlerImpl =
              new NetworkPacketTaggingHandlerImpl(
                  PrivilegedOperationExecutor.getInstance(conf), cGroupV1Handler);
        }
      }
    }
    return networkPacketTaggingHandlerImpl;
  }

  /**
   * 初始化出网带宽资源处理器。
   * @param conf YARN配置
   * @return 初始化好的处理器
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  public static OutboundBandwidthResourceHandler
      initOutboundBandwidthResourceHandler(Configuration conf)
      throws ResourceHandlerException {
    return getTrafficControlBandwidthHandler(conf);
  }

  /**
   * 初始化磁盘资源处理器。
   * @param conf YARN配置
   * @return 初始化好的处理器，未启用则返回null
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  public static DiskResourceHandler initDiskResourceHandler(Configuration conf)
      throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_DISK_RESOURCE_ENABLED)) {
      return getCgroupsBlkioResourceHandler(conf);
    }
    return null;
  }

  /**
   * 双重检查锁定初始化cgroups blkio磁盘资源处理器单例。
   * @param conf YARN配置
   * @return 初始化好的处理器
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private static CGroupsBlkioResourceHandlerImpl getCgroupsBlkioResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    if (cGroupsBlkioResourceHandler == null) {
      synchronized (DiskResourceHandler.class) {
        if (cGroupsBlkioResourceHandler == null) {
          LOG.debug("Creating new cgroups blkio handler");

          initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.BLKIO);
          cGroupsBlkioResourceHandler =
              new CGroupsBlkioResourceHandlerImpl(cGroupV1Handler);
        }
      }
    }
    return cGroupsBlkioResourceHandler;
  }

  /**
   * 初始化内存资源处理器。
   * @param conf YARN配置
   * @return 初始化好的处理器，未启用则返回null
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  public static MemoryResourceHandler initMemoryResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_ENABLED)) {
      return getCgroupsMemoryResourceHandler(conf);
    }
    return null;
  }

  /**
   * 双重检查锁定初始化内存资源处理器单例，根据cgroups版本选择对应实现。
   * @param conf YARN配置
   * @return 初始化好的处理器
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private static MemoryResourceHandler
      getCgroupsMemoryResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    if (cGroupsMemoryResourceHandler == null) {
      synchronized (MemoryResourceHandler.class) {
        if (cGroupsMemoryResourceHandler == null) {

          initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.MEMORY);
          // 根据内存控制器挂载版本选择对应处理器实现
          if (isMountedInCGroupsV2(CGroupsHandler.CGroupController.MEMORY)) {
            cGroupsMemoryResourceHandler = new CGroupsV2MemoryResourceHandlerImpl(cGroupV2Handler);
          } else {
            cGroupsMemoryResourceHandler = new CGroupsMemoryResourceHandlerImpl(cGroupV1Handler);
          }
        }
      }
    }
    return cGroupsMemoryResourceHandler;
  }

  /**
   * 根据配置初始化NUMA感知资源处理器。
   * @param conf YARN配置
   * @param nmContext NodeManager上下文
   * @return 初始化好的处理器，未启用则返回null
   */
  private static ResourceHandler getNumaResourceHandler(Configuration conf,
      Context nmContext) {
    if (YarnConfiguration.numaAwarenessEnabled(conf)) {
      return new NumaResourceHandlerImpl(conf, nmContext);
    }
    return null;
  }

  /**
   * 如果处理器不为空，则添加到处理器列表中。
   * @param handlerList 处理器列表
   * @param handler 待添加的处理器
   */
  private static void addHandlerIfNotNull(List<ResourceHandler> handlerList,
      ResourceHandler handler) {
    if (handler != null) {
      handlerList.add(handler);
    }
  }

  /**
   * 根据配置初始化完整的资源处理链，包含内置处理器和资源插件扩展处理器