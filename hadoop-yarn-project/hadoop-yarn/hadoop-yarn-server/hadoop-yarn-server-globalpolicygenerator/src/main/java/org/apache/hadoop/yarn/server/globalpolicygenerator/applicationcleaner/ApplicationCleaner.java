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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：YARN联邦全局策略生成器中，负责清理联邦状态存储中过期应用的抽象基类
 * 
 * The ApplicationCleaner is a runnable that cleans up old applications from
 * table applicationsHomeSubCluster in FederationStateStore.
 */
public abstract class ApplicationCleaner implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationCleaner.class);

  // Hadoop配置对象
  private Configuration conf;
  // 全局策略生成器上下文对象
  private GPGContext gpgContext;
  // 联邦注册表客户端，用于访问联邦状态存储
  private FederationRegistryClient registryClient;

  // 成功查询路由的最小成功次数，达到后即可返回结果
  private int minRouterSuccessCount;
  // 查询路由的最大重试次数
  private int maxRouterRetry;
  // 查询路由失败后的重试间隔(毫秒)
  private long routerQueryIntevalMillis;

  /**
   * 初始化应用清理器，加载配置参数并验证配置合法性
   * @param config Hadoop配置对象
   * @param context 全局策略生成器上下文
   * @throws YarnException 配置不合法时抛出异常
   */
  public void init(Configuration config, GPGContext context)
      throws YarnException {

    this.gpgContext = context;
    this.conf = config;
    this.registryClient = context.getRegistryClient();

    // 从配置中读取路由连接参数字符串
    String routerSpecString =
        this.conf.get(YarnConfiguration.GPG_APPCLEANER_CONTACT_ROUTER_SPEC,
            YarnConfiguration.DEFAULT_GPG_APPCLEANER_CONTACT_ROUTER_SPEC);
    // 按逗号分割三个参数：最小成功次数、最大重试次数、重试间隔
    String[] specs = routerSpecString.split(",");
    if (specs.length != 3) {
      throw new YarnException("Expect three comma separated values in "
          + YarnConfiguration.GPG_APPCLEANER_CONTACT_ROUTER_SPEC + " but get "
          + routerSpecString);
    }
    this.minRouterSuccessCount = Integer.parseInt(specs[0]);
    this.maxRouterRetry = Integer.parseInt(specs[1]);
    this.routerQueryIntevalMillis = Long.parseLong(specs[2]);

    // 验证配置：最小成功次数不能大于最大重试次数
    if (this.minRouterSuccessCount > this.maxRouterRetry) {
      throw new YarnException("minRouterSuccessCount "
          + this.minRouterSuccessCount
          + " should not be larger than maxRouterRetry" + this.maxRouterRetry);
    }
    // 验证配置：最小成功次数必须为正整数
    if (this.minRouterSuccessCount <= 0) {
      throw new YarnException("minRouterSuccessCount "
          + this.minRouterSuccessCount + " should be positive");
    }

    LOG.info("Initialized AppCleaner with Router query with min success {}, " +
        "max retry {}, retry interval {}.", this.minRouterSuccessCount,
        this.maxRouterRetry,
        DurationFormatUtils.formatDurationISO(this.routerQueryIntevalMillis));
  }

  /**
   * 获取全局策略生成器上下文
   * @return 全局策略生成器上下文对象
   */
  public GPGContext getGPGContext() {
    return this.gpgContext;
  }

  /**
   * 获取联邦注册表客户端
   * @return 联邦注册表客户端对象
   */
  public FederationRegistryClient getRegistryClient() {
    return this.registryClient;
  }

  /**
   * 调用Router REST接口获取当前集群所有应用列表
   *
   * @return Router已知的所有应用ID集合
   * @throws YarnRuntimeException 调用Router接口失败时抛出
   */
  public Set<ApplicationId> getAppsFromRouter() throws YarnRuntimeException {
    // 获取Router Web服务地址
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);

    LOG.info("Contacting router at: {}.", webAppAddress);
    // 调用Router WebService获取应用列表，排除资源请求字段减少数据传输
    AppsInfo appsInfo = GPGUtils.invokeRMWebService(webAppAddress, RMWSConsts.APPS,
        AppsInfo.class, conf, DeSelectFields.DeSelectType.RESOURCE_REQUESTS.toString());

    // 解析响应，转换为ApplicationId集合
    Set<ApplicationId> appSet = new HashSet<>();
    for (AppInfo appInfo : appsInfo.getApps()) {
      appSet.add(ApplicationId.fromString(appInfo.getAppId()));
    }
    return appSet;
  }

  /**
   * 带重试机制从Router获取集群已知应用列表，达到最小成功次数后返回结果
   *
   * @return Router已知的所有应用ID集合
   * @throws YarnException 达到最大重试次数仍未满足最小成功次数时抛出
   */
  public Set<ApplicationId> getRouterKnownApplications() throws YarnException {
    int successCount = 0, totalAttemptCount = 0;
    Set<ApplicationId> resultSet = new HashSet<>();
    // 循环重试，直到达到最大重试次数
    while (totalAttemptCount < this.maxRouterRetry) {
      try {
        // 单次调用获取应用列表
        Set<ApplicationId> routerApps = getAppsFromRouter();
        resultSet.addAll(routerApps);
        LOG.info("Attempt {}: {} known apps from Router, {} in total",
            totalAttemptCount, routerApps.size(), resultSet.size());

        // 成功计数+1，达到最小成功次数直接返回结果
        successCount++;
        if (successCount >= this.minRouterSuccessCount) {
          return resultSet;
        }

        // 重试前等待指定间隔
        try {
          Thread.sleep(this.routerQueryIntevalMillis);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted after attempt {}.", totalAttemptCount);
        }
      } catch (Exception e) {
        // 本次查询失败，记录日志后继续重试
        LOG.warn("Router query attempt {} failed.", totalAttemptCount, e);
      } finally {
        // 无论成功失败，总尝试次数+1
        totalAttemptCount++;
      }
    }
    // 达到最大重试次数仍未满足最小成功次数，抛出异常
    throw new YarnException("Only " + successCount
        + " success Router queries after " + totalAttemptCount + " retries");
  }

  @Override
  public abstract void run();
}