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

package org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：默认应用清理器实现，负责清理联邦状态存储中已经完成的旧应用元数据
 * 清理FederationStateStore中applicationsHomeSubCluster表的旧应用条目默认实现。
 */
public class DefaultApplicationCleaner extends ApplicationCleaner {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultApplicationCleaner.class);

  @Override
  public void run() {
    // 记录当前清理执行时间
    Date now = new Date();
    LOG.info("Application cleaner run at time {}", now);

    // 获取联邦状态存储门面实例
    FederationStateStoreFacade facade = getGPGContext().getStateStoreFacade();
    try {
      // 从状态存储获取所有应用，存入集合
      Set<ApplicationId> allStateStoreApps = new HashSet<>();
      List<ApplicationHomeSubCluster> response =
          facade.getApplicationsHomeSubCluster();
      for (ApplicationHomeSubCluster app : response) {
        allStateStoreApps.add(app.getApplicationId());
      }
      LOG.info("{} app entries in FederationStateStore", allStateStoreApps.size());

      // 从联邦注册中心获取所有应用列表
      List<String> allRegistryApps = getRegistryClient().getAllApplications();
      LOG.info("{} app entries in FederationRegistry", allStateStoreApps.size());

      // 从Router获取当前活跃的已知应用集合
      Set<ApplicationId> routerApps = getRouterKnownApplications();
      LOG.info("{} known applications from Router", routerApps.size());

      // 计算需要删除的应用：状态存储中有但Router未知的已完成应用
      Set<ApplicationId> toDelete =
          Sets.difference(allStateStoreApps, routerApps);

      LOG.info("Deleting {} applications from statestore", toDelete.size());
      LOG.debug("Apps to delete: {}.",
          toDelete.stream().map(Object::toString).collect(Collectors.joining(",")));

      // 遍历待删除应用，逐个从状态存储删除
      for (ApplicationId appId : toDelete) {
        try {
          LOG.debug("Deleting {} from statestore ", appId);
          facade.deleteApplicationHomeSubCluster(appId);
        } catch (Exception e) {
          LOG.error("deleteApplicationHomeSubCluster failed at application {}.", appId, e);
        }
      }

      // 清理注册中心中的过期应用条目
      for (String app : allRegistryApps) {
        ApplicationId appId = ApplicationId.fromString(app);
        if (!routerApps.contains(appId)) {
          LOG.debug("removing finished application entry for {}", app);
          getRegistryClient().removeAppFromRegistry(appId, true);
        }
      }
    } catch (Throwable e) {
      LOG.error("Application cleaner started at time {} fails. ", now, e);
    }
  }
}