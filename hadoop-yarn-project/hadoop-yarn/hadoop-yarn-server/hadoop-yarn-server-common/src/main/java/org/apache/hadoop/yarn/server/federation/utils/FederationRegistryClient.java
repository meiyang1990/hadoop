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

package org.apache.hadoop.yarn.server.federation.utils;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN联邦环境下，对接Yarn Registry的工具类，负责读写UAM（Unified Application Master）高可用相关信息，
 * 支持UAM重试和故障恢复场景。
 */
public class FederationRegistryClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRegistryClient.class);

  // Yarn Registry操作实例
  private RegistryOperations registry;

  // 当前操作Registry使用的用户身份
  private UserGroupInformation user;

  // 本地缓存：应用ID -> 子集群ID -> UAM AMRM令牌
  private Map<ApplicationId, Map<String, Token<AMRMTokenIdentifier>>>
      appSubClusterTokenMap;

  // Yarn Registry中存储联邦信息的根目录路径
  private String registryBaseDir;

  /**
   * 构造联邦Registry客户端，初始化缓存和根路径配置。
   *
   * @param conf YARN配置对象
   * @param registry Registry操作实例
   * @param user 操作Registry的用户身份
   */
  public FederationRegistryClient(Configuration conf,
      RegistryOperations registry, UserGroupInformation user) {
    this.registry = registry;
    this.user = user;
    this.appSubClusterTokenMap = new ConcurrentHashMap<>();
    this.registryBaseDir =
        conf.get(YarnConfiguration.FEDERATION_REGISTRY_BASE_KEY,
            YarnConfiguration.DEFAULT_FEDERATION_REGISTRY_BASE_KEY);
    LOG.info("Using registry {} with base directory: {}",
        this.registry.getClass().getName(), this.registryBaseDir);
  }

  /**
   * 获取Registry中所有已知应用ID列表。
   *
   * @return 所有应用ID的列表，不存在时返回空列表
   */
  public synchronized List<String> getAllApplications() {
    // Suppress the exception here because it is valid that the entry does not
    // exist
    List<String> applications = null;
    try {
      applications = listDirRegistry(this.registry, this.user,
          getRegistryKey(null, null), false);
    } catch (YarnException e) {
      LOG.warn("Unexpected exception from listDirRegistry", e);
    }
    if (applications == null) {
      // It is valid for listDirRegistry to return null
      return new ArrayList<>();
    }
    return applications;
  }

  /**
   * 清空Registry中所有应用记录，仅用于测试。
   */
  @VisibleForTesting
  public synchronized void cleanAllApplications() {
    try {
      removeKeyRegistry(this.registry, this.user, getRegistryKey(null, null),
          true, false);
    } catch (YarnException e) {
      LOG.warn("Unexpected exception from removeKeyRegistry", e);
    }
  }

  /**
   * 写入或更新指定应用子集群的UAM AMRM令牌到Registry。
   *
   * @param appId 应用ID
   * @param subClusterId 子集群ID
   * @param token UAM的AMRM令牌
   * @return 是否新增或更新了令牌（令牌未变化返回false）
   */
  public synchronized boolean writeAMRMTokenForUAM(ApplicationId appId,
      String subClusterId, Token<AMRMTokenIdentifier> token) {
    Map<String, Token<AMRMTokenIdentifier>> subClusterTokenMap =
        this.appSubClusterTokenMap.get(appId);
    if (subClusterTokenMap == null) {
      subClusterTokenMap = new ConcurrentHashMap<>();
      this.appSubClusterTokenMap.put(appId, subClusterTokenMap);
    }

    boolean update = !token.equals(subClusterTokenMap.get(subClusterId));
    if (!update) {
      LOG.debug("Same amrmToken received from {}, skip writing registry for {}",
          subClusterId, appId);
      return update;
    }

    LOG.info("Writing/Updating amrmToken for {} to registry for {}",
        subClusterId, appId);
    try {
      // 先写入令牌到Registry
      writeRegistry(this.registry, this.user,
          getRegistryKey(appId, subClusterId), token.encodeToUrlString(), true);

      // 更新本地缓存
      subClusterTokenMap.put(subClusterId, token);
    } catch (YarnException | IOException e) {
      LOG.error("Failed writing AMRMToken to registry for subcluster {}.", subClusterId, e);
    }
    return update;
  }

  /**
   * 从Registry加载指定应用的所有UAM AMRM令牌信息。
   *
   * @param appId 应用ID
   * @return 子集群ID到AMRM令牌的映射表
   */
  public synchronized Map<String, Token<AMRMTokenIdentifier>>
      loadStateFromRegistry(ApplicationId appId) {
    Map<String, Token<AMRMTokenIdentifier>> retMap = new HashMap<>();
    // Suppress the exception here because it is valid that the entry does not
    // exist
    List<String> subclusters = null;
    try {
      subclusters = listDirRegistry(this.registry, this.user,
          getRegistryKey(appId, null), false);
    } catch (YarnException e) {
      LOG.warn("Unexpected exception from listDirRegistry", e);
    }

    if (subclusters == null) {
      LOG.info("Application {} does not exist in registry", appId);
      return retMap;
    }

    // 遍历子集群逐个读取AMRM令牌
    for (String scId : subclusters) {
      LOG.info("Reading amrmToken for subcluster {} for {}", scId, appId);
      String key = getRegistryKey(appId, scId);
      try {
        String tokenString = readRegistry(this.registry, this.user, key, true);
        if (tokenString == null) {
          throw new YarnException("Null string from readRegistry key " + key);
        }
        Token<AMRMTokenIdentifier> amrmToken = new Token<>();
        amrmToken.decodeFromUrlString(tokenString);
        // 清空服务字段，模拟RM新签发令牌的状态
        amrmToken.setService(new Text());

        retMap.put(scId, amrmToken);
      } catch (Exception e) {
        LOG.error("Failed reading registry key {}, skipping subcluster {}.",  key, scId, e);
      }
    }

    // 更新本地缓存覆盖旧数据
    this.appSubClusterTokenMap.put(appId, new ConcurrentHashMap<>(retMap));
    return retMap;
  }

  /**
   * 从Registry中删除指定应用的所有记录。
   *
   * @param appId 应用ID
   */
  public synchronized void removeAppFromRegistry(ApplicationId appId) {
    removeAppFromRegistry(appId, false);
  }

  /**
   * 从Registry中删除指定应用的所有记录，可选择忽略本地缓存状态。
   *
   * @param appId 应用ID
   * @param ignoreMemoryState 是否忽略本地缓存中存储的应用状态
   */
  public synchronized void removeAppFromRegistry(ApplicationId appId,
      boolean ignoreMemoryState) {
    Map<String, Token<AMRMTokenIdentifier>> subClusterTokenMap =
        this.appSubClusterTokenMap.get(appId);
    if (!ignoreMemoryState) {
      if (MapUtils.isEmpty(subClusterTokenMap)) {
        return;
      }
    }
    LOG.info("Removing all registry entries for {}.", appId);

    // 删除应用在Registry中的目录
    String key = getRegistryKey(appId, null);
    try {
      removeKeyRegistry(this.registry, this.user, key, true, true);
      if (subClusterTokenMap != null) {
        subClusterTokenMap.clear();
      }
    } catch (YarnException e) {
      LOG.error("Failed removing registry directory key {}.", key, e);
    }
  }

  /**
   * 构造Registry中对应条目的完整路径键。
   *
   * @param appId 应用ID，传null返回根目录
   * @param fileName 文件名（子集群ID），传null返回应用目录
   * @return 完整Registry路径键
   */
  private String getRegistryKey(ApplicationId appId, String fileName) {
    if (appId == null) {
      return this.registryBaseDir;
    }
    if (fileName == null) {
      return this.registryBaseDir + appId.toString();
    }
    return this.registryBaseDir + appId.toString() + "/" + fileName;
  }

  /**
   * 在指定用户身份下读取Registry条目内容。
   *
   * @param registryImpl Registry操作实例
   * @param ugi 操作用户身份
   * @param key Registry路径键
   * @param throwIfFails 读取失败是否抛出异常
   * @return 读取到的条目描述内容
   * @throws YarnException 读取失败且throwIfFails为true时抛出
   */
  private String readRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final boolean throwIfFails)
      throws YarnException {
    // 使用带应用凭证的UGI访问Registry
    String result = ugi.doAs(new PrivilegedAction<String>() {
      @Override
      public String run() {
        try {
          ServiceRecord value = registryImpl.resolve(key);
          if (value != null) {
            return value.description;
          }
        } catch (Throwable e) {
          if (throwIfFails) {
            LOG.error("Registry resolve key {} failed.", key, e);
          }
        }
        return null;
      }
    });
    if (result == null && throwIfFails) {
      throw new YarnException("Registry resolve key " + key + " failed");
    }
    return result;
  }

  /**
   * 在指定用户身份下删除Registry条目。
   *
   * @param registryImpl Registry操作实例
   * @param ugi 操作用户身份
   * @param key Registry路径键
   * @param recursive 是否递归删除子条目
   * @param throwIfFails 删除失败是否抛出异常
   * @throws YarnException 删除失败且throwIfFails为true时抛出
   */
  private void removeKeyRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final boolean recursive,
      final boolean throwIfFails) throws YarnException {
    // 使用带应用凭证的UGI访问Registry
    boolean success = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          registryImpl.delete(key, recursive);
          return true;
        } catch (Throwable e) {
          if (throwIfFails) {
            LOG.error("Registry remove key {} failed.", key, e);
          }
        }
        return false;
      }
    });
    if (!success && throwIfFails) {
      throw new YarnException("Registry remove key " + key + " failed");
    }
  }

  /**
   * 在指定用户身份下写入Registry条目，存在则覆盖。
   *
   * @param registryImpl Registry操作实例
   * @param ugi 操作用户身份
   * @param key Registry路径键
   * @param value 要写入的内容
   * @param throwIfFails 写入失败是否抛出异常
   * @throws YarnException 写入失败且throwIfFails为true时抛出
   */
  private void writeRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final String value,
      final boolean throwIfFails) throws YarnException {

    final ServiceRecord recordValue = new ServiceRecord();
    recordValue.description = value;
    // 使用带应用凭证的UGI访问Registry
    boolean success = ugi.doAs(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          registryImpl.bind(key, recordValue, BindFlags.OVERWRITE);
          return true;
        } catch (Throwable e) {
          if (throwIfFails) {
            LOG.error("Registry write key {} failed.", key, e);
          }
        }
        return false;
      }
    });
    if (!success && throwIfFails) {
      throw new YarnException("Registry write key " + key + " failed");
    }
  }

  /**
   * 在指定用户身份下列出指定目录的所有子条目。
   *
   * @param registryImpl Registry操作实例
   * @param ugi 操作用户身份
   * @param key Registry路径键
   * @param throwIfFails 列出失败是否抛出异常
   * @return 子条目名称列表
   * @throws YarnException 列出失败且throwIfFails为true时抛出
   */
  private List<String> listDirRegistry(final RegistryOperations registryImpl,
      UserGroupInformation ugi, final String key, final boolean throwIfFails)
      throws YarnException {
    List<String> result = ugi.doAs((PrivilegedAction<List<String>>) () -> {
      try {
        return registryImpl.list(key);
      } catch (Throwable e) {
        if (throwIfFails) {
          LOG.error("Registry list key {} failed.", key, e);
        }
      }
      return null;
    });
    if (result == null && throwIfFails) {
      throw new YarnException("Registry list key " + key + " failed");
    }
    return result;
  }

}