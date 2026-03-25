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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.classification.VisibleForTesting;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YARNFeatureNotEnabledException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 资源配置文件管理器实现，负责管理YARN集群中预定义的资源配置文件，支持从配置文件加载和查询资源规格
 */
public class ResourceProfilesManagerImpl implements ResourceProfilesManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceProfilesManagerImpl.class);

  // 存储所有已加载的资源配置文件，key为配置名称，value为对应的资源规格
  private final Map<String, Resource> profiles = new ConcurrentHashMap<>();
  private Configuration conf;
  // 标记资源配置文件功能是否启用
  private boolean profileEnabled = false;

  private static final String MEMORY = ResourceInformation.MEMORY_MB.getName();
  private static final String VCORES = ResourceInformation.VCORES.getName();

  public static final String DEFAULT_PROFILE = "default";
  public static final String MINIMUM_PROFILE = "minimum";
  public static final String MAXIMUM_PROFILE = "maximum";

  protected final ReentrantReadWriteLock.ReadLock readLock;
  protected final ReentrantReadWriteLock.WriteLock writeLock;

  private static final String[] MANDATORY_PROFILES = {DEFAULT_PROFILE,
      MINIMUM_PROFILE, MAXIMUM_PROFILE};
  private static final String FEATURE_NOT_ENABLED_MSG =
      "Resource profile is not enabled, please "
          + "enable resource profile feature before using its functions."
          + " (by setting " + YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED
          + " to true)";

  /**
   * 构造函数，初始化读写锁用于并发访问控制
   */
  public ResourceProfilesManagerImpl() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * 初始化资源配置文件管理器，加载配置文件
   * @param config YARN配置对象
   * @throws IOException 加载配置文件失败时抛出异常
   */
  public void init(Configuration config) throws IOException {
    conf = config;
    loadProfiles();
  }

  /**
   * 从配置文件加载所有资源配置
   * @throws IOException 配置解析或读取失败时抛出异常
   */
  private void loadProfiles() throws IOException {
    // 读取配置判断是否启用资源配置文件功能
    profileEnabled =
        conf.getBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED,
            YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_ENABLED);
    if (!profileEnabled) {
      return;
    }
    // 获取资源配置文件路径
    String sourceFile =
        conf.get(YarnConfiguration.RM_RESOURCE_PROFILES_SOURCE_FILE,
            YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_SOURCE_FILE);
    String resourcesFile = sourceFile;
    // 获取类加载器，优先使用当前线程上下文类加载器
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ResourceProfilesManagerImpl.class.getClassLoader();
    }
    // 从类路径查找配置文件
    if (classLoader != null) {
      URL tmp = classLoader.getResource(sourceFile);
      if (tmp != null) {
        resourcesFile = tmp.getPath();
      }
    }
    // 使用Jackson解析JSON格式配置文件
    ObjectMapper mapper = new ObjectMapper();
    Map data = mapper.readValue(new File(resourcesFile), Map.class);
    Iterator iterator = data.entrySet().iterator();
    // 遍历所有配置项逐个解析
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      String profileName = entry.getKey().toString();
      if (profileName.isEmpty()) {
        throw new IOException(
            "Name of resource profile cannot be an empty string");
      }

      // 最小/最大配置禁止在用户配置文件中定义，会从resource-types.xml自动加载
      if (profileName.equals(MINIMUM_PROFILE) || profileName.equals(
          MAXIMUM_PROFILE)) {
        throw new IOException(String.format(
            "profile={%s, %s} is should not be specified "
                + "inside %s, they will be loaded from resource-types.xml",
            MINIMUM_PROFILE, MAXIMUM_PROFILE, sourceFile));
      }
      // 解析单个资源配置信息
      if (entry.getValue() instanceof Map) {
        Map profileInfo = (Map) entry.getValue();
        // 检查必须包含内存和CPU核心两个资源项
        if (!profileInfo.containsKey(MEMORY)
            || !profileInfo.containsKey(VCORES)) {
          throw new IOException(
              "Illegal resource profile definition; profile '" + profileName
                  + "' must contain '" + MEMORY + "' and '" + VCORES + "'");
        }
        Resource resource = parseResource(profileInfo);
        profiles.put(profileName, resource);
        LOG.info(
            "Added profile '" + profileName + "' with resources: " + resource);
      }
    }

    // 从全局资源配置加载最小/最大资源分配配置并添加到配置列表
    profiles.put(MINIMUM_PROFILE,
        ResourceUtils.getResourceTypesMinimumAllocation());
    profiles.put(MAXIMUM_PROFILE,
        ResourceUtils.getResourceTypesMaximumAllocation());

    // 检查所有必须的配置是否都存在
    for (String profile : MANDATORY_PROFILES) {
      if (!profiles.containsKey(profile)) {
        throw new IOException(
            "Mandatory profile missing '" + profile + "' missing. "
                + Arrays.toString(MANDATORY_PROFILES) + " must be present");
      }
    }
    LOG.info("Loaded profiles: " + profiles.keySet());
  }

  /**
   * 从配置Map解析构建Resource对象
   * @param profileInfo 单个资源配置的键值对
   * @return 解析完成的Resource对象
   * @throws IOException 资源类型不识别时抛出异常
   */
  private Resource parseResource(Map profileInfo) throws IOException {
    Resource resource = Resource.newInstance(0, 0);
    Iterator iterator = profileInfo.entrySet().iterator();
    // 获取集群已注册的所有资源类型
    Map<String, ResourceInformation> resourceTypes = ResourceUtils
        .getResourceTypes();
    while (iterator.hasNext()) {
      Map.Entry resourceEntry = (Map.Entry) iterator.next();
      String resourceName = resourceEntry.getKey().toString();
      // 将字符串值转换为ResourceInformation对象
      ResourceInformation resourceValue = fromString(resourceName,
          resourceEntry.getValue().toString());
      // 单独设置内存大小
      if (resourceName.equals(MEMORY)) {
        resource.setMemorySize(resourceValue.getValue());
        continue;
      }
      // 单独设置CPU核心数
      if (resourceName.equals(VCORES)) {
        resource
            .setVirtualCores(Long.valueOf(resourceValue.getValue()).intValue());
        continue;
      }
      // 处理自定义资源类型
      if (resourceTypes.containsKey(resourceName)) {
        resource.setResourceInformation(resourceName, resourceValue);
      } else {
        throw new IOException("Unrecognized resource type '" + resourceName
            + "'. Recognized resource types are '" + resourceTypes.keySet()
            + "'");
      }
    }
    return resource;
  }

  /**
   * 检查资源配置功能是否启用，未启用则抛出异常
   * @throws YARNFeatureNotEnabledException 功能未启用时抛出异常
   */
  private void checkAndThrowExceptionWhenFeatureDisabled()
      throws YARNFeatureNotEnabledException {
    if (!profileEnabled) {
      throw new YARNFeatureNotEnabledException(FEATURE_NOT_ENABLED_MSG);
    }
  }

  @Override
  public Resource getProfile(String profile) throws YarnException{
    // 检查功能是否启用
    checkAndThrowExceptionWhenFeatureDisabled();

    if (profile == null) {
      throw new YarnException("Profile name cannot be null");
    }

    Resource profileRes = profiles.get(profile);
    if (profileRes == null) {
      throw new YarnException(
          "Resource profile '" + profile + "' not found");
    }
    // 返回克隆对象避免外部修改内部缓存
    return Resources.clone(profileRes);
  }

  @Override
  public Map<String, Resource> getResourceProfiles()
      throws YARNFeatureNotEnabledException {
    checkAndThrowExceptionWhenFeatureDisabled();
    // 返回不可修改视图避免外部修改内部缓存
    return Collections.unmodifiableMap(profiles);
  }

  @Override
  @VisibleForTesting
  public void reloadProfiles() throws IOException {
    profiles.clear();
    loadProfiles();
  }

  @Override
  public Resource getDefaultProfile() throws YarnException {
    return getProfile(DEFAULT_PROFILE);
  }

  @Override
  public Resource getMinimumProfile() throws YarnException {
    return getProfile(MINIMUM_PROFILE);
  }

  @Override
  public Resource getMaximumProfile() throws YarnException {
    return getProfile(MAXIMUM_PROFILE);
  }

  /**
   * 从带单位的字符串解析资源值，构建ResourceInformation对象
   * @param name 资源名称
   * @param value 带单位的资源值字符串
   * @return 构建完成的ResourceInformation对象
   */
  private ResourceInformation fromString(String name, String value) {
    String units = ResourceUtils.getUnits(value);
    Long resourceValue =
        Long.valueOf(value.substring(0, value.length() - units.length()));
    return ResourceInformation.newInstance(name, units, resourceValue);
  }
}