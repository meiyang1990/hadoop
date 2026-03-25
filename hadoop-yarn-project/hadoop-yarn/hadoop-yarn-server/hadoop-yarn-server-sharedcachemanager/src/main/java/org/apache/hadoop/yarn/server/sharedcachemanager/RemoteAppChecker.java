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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;

// 这个文件已经全部加上中文注释
// 远程应用检查器实现，通过查询资源管理器远程判断应用是否仍在运行
/**
 * 共享缓存管理器的应用状态检查器实现，通过远程查询ResourceManager判断应用是否活跃。
 * 用于共享缓存清理时判断缓存资源是否还被活跃应用使用。
 */
@Private
@Unstable
public class RemoteAppChecker extends AppChecker {

  // 所有活跃应用状态集合，处于这些状态的应用被认为正在运行
  private static final EnumSet<YarnApplicationState> ACTIVE_STATES = EnumSet
      .of(YarnApplicationState.NEW, YarnApplicationState.ACCEPTED,
          YarnApplicationState.NEW_SAVING, YarnApplicationState.SUBMITTED,
          YarnApplicationState.RUNNING);

  // Yarn客户端实例，用于和ResourceManager通信
  private final YarnClient client;

  /**
   * 默认构造函数，自动创建YarnClient实例。
   */
  public RemoteAppChecker() {
    this(YarnClient.createYarnClient());
  }

  /**
   * 带参数构造函数，使用外部传入的YarnClient实例（用于测试注入）。
   * @param client 预配置的YarnClient实例
   */
  RemoteAppChecker(YarnClient client) {
    super("RemoteAppChecker");
    this.client = client;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 将YarnClient添加为服务子组件，由服务框架统一管理生命周期
    addService(client);
    super.serviceInit(conf);
  }

  @Override
  @Private
  /**
   * 判断指定应用ID是否处于活跃运行状态。
   * @param id 待检查的应用ID
   * @return true如果应用活跃，false如果应用不存在或已结束
   * @throws YarnException 客户端与ResourceManager通信异常时抛出
   */
  public boolean isApplicationActive(ApplicationId id) throws YarnException {
    ApplicationReport report = null;
    try {
      // 远程查询应用报告
      report = client.getApplicationReport(id);
    } catch (ApplicationNotFoundException e) {
      // 应用不存在，返回非活跃
      return false;
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (report == null) {
      // 未查询到应用信息，返回非活跃
      return false;
    }

    // 判断应用状态是否属于活跃状态集合
    return ACTIVE_STATES.contains(report.getYarnApplicationState());
  }

  @Override
  @Private
  /**
   * 获取集群中所有处于活跃状态的应用ID列表。
   * @return 所有活跃应用ID的集合
   * @throws YarnException 客户端与ResourceManager通信异常时抛出
   */
  public Collection<ApplicationId> getActiveApplications() throws YarnException {
    try {
      List<ApplicationId> activeApps = new ArrayList<ApplicationId>();
      // 远程查询所有处于活跃状态的应用报告
      List<ApplicationReport> apps = client.getApplications(ACTIVE_STATES);
      // 提取所有应用ID
      for (ApplicationReport app: apps) {
        activeApps.add(app.getApplicationId());
      }
      return activeApps;
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }
}