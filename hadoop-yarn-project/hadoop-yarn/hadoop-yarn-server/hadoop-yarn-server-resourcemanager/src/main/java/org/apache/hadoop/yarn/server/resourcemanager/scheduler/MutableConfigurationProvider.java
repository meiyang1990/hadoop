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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;

/**
 * YARN调度器可修改配置提供者接口，定义支持动态修改调度器配置的标准方法。
 */
public interface MutableConfigurationProvider {

  /**
   * 获取配置修改ACL访问控制策略。
   * @return 配置修改ACL访问控制策略实例
   */
  ConfigurationMutationACLPolicy getAclMutationPolicy();

  /**
   * 当ResourceManager启动或成为活跃节点时调用，确保配置为最新版本。
   * @throws Exception 从持久化存储刷新配置失败时抛出
   */
  void reloadConfigurationFromStore() throws Exception;

  /**
   * 记录用户请求的配置变更，并将其应用到内存配置中。
   * @param user 请求修改配置的用户
   * @param confUpdate 用户请求的配置变更信息
   * @return 包含变更信息的日志记录对象
   * @throws Exception 记录变更失败时抛出
   */
  LogMutation logAndApplyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws Exception;

  /**
   * 将变更合并到原始配置，生成新的配置对象。
   * @param oldConfiguration 原始基础配置
   * @param confUpdate 配置变更列表
   * @return 应用变更后的新配置对象
   * @throws IOException 合并变更失败时抛出
   */
  Configuration applyChanges(Configuration oldConfiguration,
                             SchedConfUpdateInfo confUpdate) throws IOException;

  /**
   * 确认上一条记录的配置变更，持久化到存储。
   * @param pendingMutation 待确认的配置变更记录
   * @param isValid 标识变更是否已正确应用到调度器
   * @throws Exception 确认变更失败时抛出
   */
  void confirmPendingMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception;

  /**
   * 获取当前提供者缓存的调度器配置。
   * @return 缓存的调度器配置
   */
  Configuration getConfiguration();

  /**
   * 获取调度器配置最后更新的版本号。
   * @return 最后更新的配置版本号
   * @throws Exception 获取版本号过程发生异常
   */
  long getConfigVersion() throws Exception;

  /**
   * 格式化持久化存储中的配置存储。
   * @param conf 用于格式化的配置参数
   * @throws Exception 格式化失败时抛出
   */
  void formatConfigurationInStore(Configuration conf) throws Exception;

  /**
   * 将配置回退到指定的旧版本配置。
   * @param config 要回退到的旧配置
   * @throws Exception 回退失败时抛出
   */
  void revertToOldConfig(Configuration config) throws Exception;

  /**
   * 关闭配置提供者，释放占用的资源。
   * @throws IOException 关闭失败时抛出
   */
  void close() throws IOException;
}