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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.DeviceMappingManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.DevicePluginAdapter;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDiscoverer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuNodeResourceUpdateHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuResourcePlugin;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

/**
 * NodeManager端资源插件管理器，负责加载、初始化和管理配置的各类扩展资源插件，
 * 支持内置GPU/FPGA资源插件和可插拔第三方设备插件两种模式。
 */
public class ResourcePluginManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ResourcePluginManager.class);
  // 支持的内置资源插件集合
  private static final Set<String> SUPPORTED_RESOURCE_PLUGINS =
      ImmutableSet.of(GPU_URI, FPGA_URI);

  // 已配置好的资源插件映射，key为资源名称，value为插件实例
  private Map<String, ResourcePlugin> configuredPlugins =
          Collections.emptyMap();

  // 设备映射管理器，用于可插拔设备框架管理设备分配信息
  private DeviceMappingManager deviceMappingManager = null;

  /**
   * 初始化资源插件管理器，加载所有配置的资源插件。
   * @param context NodeManager上下文对象
   * @throws YarnException 初始化异常
   * @throws ClassNotFoundException 找不到插件类异常
   */
  public void initialize(Context context)
      throws YarnException, ClassNotFoundException {
    Configuration conf = context.getConf();
    // 从配置中获取需要加载的资源插件列表
    String[] plugins = getPluginsFromConfig(conf);

    Map<String, ResourcePlugin> pluginMap = Maps.newHashMap();
    if (plugins != null) {
      // 初始化内置资源插件
      pluginMap = initializePlugins(conf, context, plugins);
    }

    // 尝试加载可插拔第三方设备插件框架
    boolean pluggableDeviceFrameworkEnabled = conf.getBoolean(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        YarnConfiguration.DEFAULT_NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED);

    if (pluggableDeviceFrameworkEnabled) {
      // 初始化可插拔第三方设备插件
      initializePluggableDevicePlugins(context, conf, pluginMap);
    } else {
      LOG.info("The pluggable device framework is not enabled."
              + " If you want, please set true to {}",
          YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED);
    }
    // 将插件映射转为不可修改，避免运行时被意外修改
    configuredPlugins = Collections.unmodifiableMap(pluginMap);
  }

  /**
   * 从配置中读取资源插件列表。
   * @param conf 配置对象
   * @return 资源插件名称数组
   */
  private String[] getPluginsFromConfig(Configuration conf) {
    String[] plugins = conf.getStrings(YarnConfiguration.NM_RESOURCE_PLUGINS);
    if (plugins == null || plugins.length == 0) {
      LOG.info("No Resource plugins found from configuration!");
    }
    LOG.info("Found Resource plugins from configuration: "
        + Arrays.toString(plugins));

    return plugins;
  }

  /**
   * 初始化内置GPU/FPGA资源插件。
   * @param conf 配置对象
   * @param context NodeManager上下文
   * @param plugins 插件名称数组
   * @return 初始化完成的插件映射
   * @throws YarnException 初始化异常
   */
  private Map<String, ResourcePlugin> initializePlugins(Configuration conf,
      Context context, String[] plugins) throws YarnException {
    Map<String, ResourcePlugin> pluginMap = Maps.newHashMap();

    for (String resourceName : plugins) {
      resourceName = resourceName.trim();
      // 检查插件是否在支持列表中
      ensurePluginIsSupported(resourceName);

      // 检查是否重复配置
      if (!isPluginDuplicate(pluginMap, resourceName)) {
        ResourcePlugin plugin = null;
        // 初始化GPU资源插件
        if (resourceName.equals(GPU_URI)) {
          final GpuDiscoverer gpuDiscoverer = new GpuDiscoverer();
          final GpuNodeResourceUpdateHandler updateHandler =
              new GpuNodeResourceUpdateHandler(gpuDiscoverer, conf);
          plugin = new GpuResourcePlugin(updateHandler, gpuDiscoverer);
        } else if (resourceName.equals(FPGA_URI)) {
          // 初始化FPGA资源插件
          plugin = new FpgaResourcePlugin();
        }

        if (plugin == null) {
          throw new YarnException(
              "This shouldn't happen, plugin=" + resourceName
                  + " should be loaded and initialized");
        }
        // 调用插件初始化方法
        plugin.initialize(context);
        LOG.info("Initialized plugin {}", plugin);
        pluginMap.put(resourceName, plugin);
      }
    }
    return pluginMap;
  }

  /**
   * 检查资源插件是否被支持。
   * @param resourceName 资源名称
   * @throws YarnException 不支持时抛出异常
   */
  private void ensurePluginIsSupported(String resourceName)
      throws YarnException {
    if (!SUPPORTED_RESOURCE_PLUGINS.contains(resourceName)) {
      String msg =
          "Trying to initialize resource plugin with name=" + resourceName
              + ", it is not supported, list of supported plugins:"
              + StringUtils.join(",", SUPPORTED_RESOURCE_PLUGINS);
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  /**
   * 检查插件是否重复配置。
   * @param pluginMap 当前已加载插件映射
   * @param resourceName 资源名称
   * @return 是否重复
   */
  private boolean isPluginDuplicate(Map<String, ResourcePlugin> pluginMap,
      String resourceName) {
    if (pluginMap.containsKey(resourceName)) {
      LOG.warn("Ignoring duplicate Resource plugin definition: " +
          resourceName);
      return true;
    }
    return false;
  }

  /**
   * 初始化可插拔第三方设备插件，加载用户配置的自定义设备插件。
   * @param context NodeManager上下文
   * @param configuration 配置对象
   * @param pluginMap 插件映射表，加载后的插件会存入此处
   * @throws YarnRuntimeException 初始化异常
   * @throws ClassNotFoundException 找不到插件类异常
   */
  public void initializePluggableDevicePlugins(Context context,
      Configuration configuration,
      Map<String, ResourcePlugin> pluginMap)
      throws YarnRuntimeException, ClassNotFoundException {
    LOG.info("The pluggable device framework enabled,"
        + "trying to load the vendor plugins");
    // 延迟创建设备映射管理器
    if (null == deviceMappingManager) {
      LOG.debug("DeviceMappingManager initialized.");
      deviceMappingManager = new DeviceMappingManager(context);
    }
    // 从配置获取第三方设备插件类名列表
    String[] pluginClassNames = configuration.getStrings(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES);
    if (null == pluginClassNames) {
      throw new YarnRuntimeException("Null value found in configuration: "
          + YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES);
    }

    // 遍历加载每个第三方设备插件
    for (String pluginClassName : pluginClassNames) {
      // 加载插件类
      Class<?> pluginClazz = Class.forName(pluginClassName);
      // 检查是否实现了DevicePlugin接口
      if (!DevicePlugin.class.isAssignableFrom(pluginClazz)) {
        throw new YarnRuntimeException("Class: " + pluginClassName
            + " not instance of " + DevicePlugin.class.getCanonicalName());
      }
      // 初始化前检查接口方法兼容性，确保所有必要方法都已实现
      checkInterfaceCompatibility(DevicePlugin.class, pluginClazz);

      // 创建插件实例
      DevicePlugin dpInstance =
          (DevicePlugin) ReflectionUtils.newInstance(
              pluginClazz, configuration);

      // 尝试向NodeManager注册插件
      // TODO: handle the plugin method timeout issue
      DeviceRegisterRequest request = null;
      try {
        // 获取插件注册信息
        request = dpInstance.getRegisterRequestInfo();
      } catch (Exception e) {
        throw new YarnRuntimeException("Exception thrown from plugin's"
            + " getRegisterRequestInfo:"
            + e.getMessage());
      }
      String resourceName = request.getResourceName();
      // 检查资源名称是否已被注册
      if (pluginMap.containsKey(resourceName)) {
        throw new YarnRuntimeException(resourceName
            + " already registered! Please change resource type name"
            + " or configure correct resource type name"
            + " in resource-types.xml for "
            + pluginClassName);
      }
      // 检查资源名称是否已在resource-types.xml中配置
      if (!isConfiguredResourceName(resourceName)) {
        throw new YarnRuntimeException(resourceName
            + " is not configured inside "
            + YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE
            + " , please configure it first");
      }
      LOG.info("New resource type: {} registered successfully by {}",
          resourceName,
          pluginClassName);
      // 创建插件适配器，将第三方DevicePlugin适配为ResourcePlugin接口
      DevicePluginAdapter pluginAdapter = new DevicePluginAdapter(
          resourceName, dpInstance, deviceMappingManager);
      LOG.info("Adapter of {} created. Initializing..", pluginClassName);
      try {
        // 初始化适配器
        pluginAdapter.initialize(context);
      } catch (YarnException e) {
        throw new YarnRuntimeException("Adapter of "
            + pluginClassName + " init failed!");
      }
      LOG.info("Adapter of {} init success!", pluginClassName);
      // 将适配器存入插件映射
      pluginMap.put(request.getResourceName(), pluginAdapter);
      // 如果插件实现了自定义调度接口，注册调度器
      if (dpInstance instanceof DevicePluginScheduler) {
        // 检查调度接口方法兼容性
        checkInterfaceCompatibility(DevicePluginScheduler.class, pluginClazz);
        LOG.info(
            "{} can schedule {} devices."
                + "Added as preferred device plugin scheduler",
            pluginClassName,
            resourceName);
        // 注册自定义设备调度器到设备映射管理器
        deviceMappingManager.addDevicePluginScheduler(
            resourceName,
            (DevicePluginScheduler) dpInstance);
      }
    } // end for
  }

  @VisibleForTesting
  /**
   * 检查插件实现类是否完整实现了目标接口的所有方法，保证接口兼容性。
   * @param expectedClass 期望实现的接口类
   * @param actualClass 实际插件实现类
   * @throws YarnRuntimeException 缺少方法时抛出异常
   */
  // Check if the implemented interfaces' signature is compatible
  public void checkInterfaceCompatibility(Class<?> expectedClass,
      Class<?> actualClass) throws YarnRuntimeException{
    LOG.debug("Checking implemented interface's compatibility: {}",
        expectedClass.getSimpleName());
    Method[] expectedDevicePluginMethods = expectedClass.getMethods();

    // 检查所有接口方法是否都存在
    boolean found;
    for (Method method: expectedDevicePluginMethods) {
      found = false;
      LOG.debug("Try to find method: {}",
          method.getName());
      for (Method m : actualClass.getDeclaredMethods()) {
        if (m.getName().equals(method.getName())) {
          LOG.debug("Method {} found in class {}",
              m.getName(), actualClass.getSimpleName());
          found = true;
          break;
        }
      }
      if (!found) {
        LOG.error("Method {} is not found in plugin",
            method.getName());
        throw new YarnRuntimeException(
            "Method " + method.getName()
                + " is expected but not implemented in "
                + actualClass.getCanonicalName());
      }
    }// end for
    LOG.info("{} compatibility is ok.",
        expectedClass.getSimpleName());
  }

  @VisibleForTesting
  /**
   * 检查资源名称是否已在全局资源配置中配置。
   * @param resourceName 资源名称
   * @return 是否已配置
   */
  public boolean isConfiguredResourceName(String resourceName) {
    // check configured
    Map<String, ResourceInformation> configuredResourceTypes =
        ResourceUtils.getResourceTypes();
    if (!configuredResourceTypes.containsKey(resourceName)) {
      return false;
    }
    return true;
  }

  @VisibleForTesting
  /**
   * 设置设备映射管理器，用于测试注入。
   * @param deviceMappingManager 设备映射管理器实例
   */
  public void setDeviceMappingManager(
      DeviceMappingManager deviceMappingManager) {
    this.deviceMappingManager = deviceMappingManager;
  }

  /**
   * 获取当前设备映射管理器。
   * @return 设备映射管理器实例
   */
  public DeviceMappingManager getDeviceMappingManager() {
    return deviceMappingManager;
  }

  /**
   * 清理所有已加载插件，释放资源。
   * @throws YarnException 清理异常
   */
  public void cleanup() throws YarnException {
    for (ResourcePlugin plugin : configuredPlugins.values()) {
      plugin.cleanup();
    }
  }

  /**
   * Get resource name (such as gpu/fpga) to plugin references.
   * @return read-only map of resource name to plugins.
   */
  public synchronized Map<String, ResourcePlugin> getNameToPlugins() {
    return configuredPlugins;
  }
}