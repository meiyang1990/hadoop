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

package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateVersionIncompatibleException;
import org.apache.hadoop.yarn.server.records.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN联邦集群状态存储核心接口，聚合了联邦集群所需的各类状态存储能力，
 * 统一管理应用位置、子集群成员、路由策略、预留信息、代理令牌等联邦元数据。
 */
public interface FederationStateStore extends
    FederationApplicationHomeSubClusterStore, FederationMembershipStateStore,
    FederationPolicyStore, FederationReservationHomeSubClusterStore,
    FederationDelegationTokenStateStore {

  Logger LOG = LoggerFactory.getLogger(FederationStateStore.class);

  /**
   * 初始化联邦状态存储，加载配置信息。
   *
   * @param conf 集群配置对象
   * @throws YarnException 初始化失败时抛出
   */
  void init(Configuration conf) throws YarnException;

  /**
   * 关闭状态存储，执行资源清理操作。
   *
   * @throws Exception 清理资源失败时抛出
   */
  void close() throws Exception;

  /**
   * 获取当前联邦状态存储客户端的版本。
   *
   * @return 当前客户端版本
   */
  Version getCurrentVersion();

  /**
   * 从持久化存储中加载已存储的联邦状态版本信息。
   *
   * @return 已存储的版本信息
   * @throws Exception 加载版本信息失败时抛出
   */
  Version loadVersion() throws Exception;

  /**
   * 将当前版本信息持久化存储到联邦状态存储中。
   *
   * @throws Exception 存储版本信息失败时抛出
   */
  void storeVersion() throws Exception;

  /**
   * 检查存储版本与当前客户端版本的兼容性，不兼容则抛出异常。
   *
   * @throws Exception 版本检查不兼容或检查过程出错时抛出
   */
  default void checkVersion() throws Exception {
    // 加载存储中已有的版本信息
    Version loadedVersion = loadVersion();
    LOG.info("Loaded Router State Version Info = {}.", loadedVersion);
    // 获取当前客户端版本
    Version currentVersion = getCurrentVersion();
    // 版本完全一致，直接返回
    if (loadedVersion != null && loadedVersion.equals(currentVersion)) {
      return;
    }
    // 无已存储版本信息，当作当前版本处理
    if (loadedVersion == null) {
      loadedVersion = currentVersion;
    }
    // 版本兼容，更新存储版本为当前版本
    if (loadedVersion.isCompatibleTo(currentVersion)) {
      LOG.info("Storing Router State Version Info {}.", currentVersion);
      storeVersion();
    } else {
      // 版本不兼容，抛出异常
      throw new FederationStateVersionIncompatibleException(
         "Expecting Router state version " + currentVersion +
         ", but loading version " + loadedVersion);
    }
  }

  /**
   * 清空状态存储中的所有数据。
   *
   * @throws Exception 清空存储失败时抛出
   */
  void deleteStateStore() throws Exception;
}