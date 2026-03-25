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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreService.HistoryServerState;
import org.apache.hadoop.mapreduce.v2.hs.server.HSAdminServer;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/******************************************************************
 * {@link JobHistoryServer} 负责处理客户端所有与作业历史相关的请求，提供已完成MapReduce作业的元数据查询和聚合日志管理能力
 *
 *****************************************************************/
/**
 * 作业历史服务器，提供已完成MapReduce作业的历史数据存储、查询和聚合日志管理服务，是MapReduce架构的核心组件之一
 */
public class JobHistoryServer extends CompositeService {

  /**
   * 作业历史服务器关闭钩子的优先级
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /**
   * 作业历史服务器启动时间戳
   */
  public static final long historyServerTimeStamp = System.currentTimeMillis();

  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryServer.class);
  private HistoryClientService clientService;
  private JobHistory jobHistoryService;
  protected JHSDelegationTokenSecretManager jhsDTSecretManager;
  private AggregatedLogDeletionService aggLogDelService;
  private HSAdminServer hsAdminServer;
  private HistoryServerStateStoreService stateStore;
  private JvmPauseMonitor pauseMonitor;

  // utility class to start and stop secret manager as part of service
  // framework and implement state recovery for secret manager on startup
  /**
   * 历史服务器令牌密钥管理器服务，将令牌密钥管理器纳入Hadoop服务框架管理，实现启动时状态恢复
   */
  private class HistoryServerSecretManagerService
      extends AbstractService {

    public HistoryServerSecretManagerService() {
      super(HistoryServerSecretManagerService.class.getName());
    }

    @Override
    protected void serviceStart() throws Exception {
      // 读取配置判断是否启用恢复功能
      boolean recoveryEnabled = getConfig().getBoolean(
          JHAdminConfig.MR_HS_RECOVERY_ENABLE,
          JHAdminConfig.DEFAULT_MR_HS_RECOVERY_ENABLE);
      if (recoveryEnabled) {
        // 恢复功能启用时，从状态存储加载之前的状态恢复令牌密钥管理器
        assert stateStore.isInState(STATE.STARTED);
        HistoryServerState state = stateStore.loadState();
        jhsDTSecretManager.recover(state);
      }

      try {
        // 启动密钥管理器线程
        jhsDTSecretManager.startThreads();
      } catch(IOException io) {
        LOG.error("Error while starting the Secret Manager threads", io);
        throw io;
      }

      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      // 停止密钥管理器线程
      if (jhsDTSecretManager != null) {
        jhsDTSecretManager.stopThreads();
      }
      super.serviceStop();
    }
  }

  /**
   * 构造作业历史服务器实例
   */
  public JobHistoryServer() {
    super(JobHistoryServer.class.getName());
  }

  @Override
  /**
   * 初始化作业历史服务器所有核心服务，完成安全登录、指标初始化和服务注册
   */
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration config = new YarnConfiguration(conf);

    // This is required for WebApps to use https if enabled.
    // 初始化WebApp的SSL配置，支持HTTPS
    MRWebAppUtil.initialize(getConfig());
    try {
      // 完成安全认证登录（安全模式下）
      doSecureLogin(conf);
    } catch(IOException ie) {
      throw new YarnRuntimeException("History Server Failed to login", ie);
    }
    // 创建作业历史核心服务
    jobHistoryService = new JobHistory();
    // 创建状态存储服务，用于支持JHS重启恢复
    stateStore = createStateStore(conf);
    // 创建代理令牌密钥管理器
    this.jhsDTSecretManager = createJHSSecretManager(conf, stateStore);
    // 创建历史客户端服务，处理客户端查询请求
    clientService = createHistoryClientService();
    // 创建聚合日志删除服务，负责清理过期聚合日志
    aggLogDelService = new AggregatedLogDeletionService();
    // 创建JHS管理服务端，处理管理命令
    hsAdminServer = new HSAdminServer(aggLogDelService, jobHistoryService);
    // 将各个子服务添加到复合服务中统一管理
    addService(stateStore);
    addService(new HistoryServerSecretManagerService());
    addService(jobHistoryService);
    addService(clientService);
    addService(aggLogDelService);
    addService(hsAdminServer);

    // 初始化指标系统，JVM指标监控
    DefaultMetricsSystem.initialize("JobHistoryServer");
    JvmMetrics jm = JvmMetrics.initSingleton("JobHistoryServer", null);
    // 创建并添加JVM暂停监控服务
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    super.serviceInit(config);
  }

  @VisibleForTesting
  /**
   * 创建历史客户端服务实例
   * @return 历史客户端服务实例
   */
  protected HistoryClientService createHistoryClientService() {
    return new HistoryClientService(jobHistoryService, this.jhsDTSecretManager);
  }

  /**
   * 根据配置创建JHS代理令牌密钥管理器，配置密钥更新周期等参数
   * @param conf 配置对象
   * @param store 历史服务器状态存储服务
   * @return 创建完成的JHS代理令牌密钥管理器实例
   */
  protected JHSDelegationTokenSecretManager createJHSSecretManager(
      Configuration conf, HistoryServerStateStoreService store) {
    long secretKeyInterval = 
        conf.getLong(MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_KEY, 
                     MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
      long tokenMaxLifetime =
        conf.getLong(MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                     MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
      long tokenRenewInterval =
        conf.getLong(MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 
                     MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
      
    return new JHSDelegationTokenSecretManager(secretKeyInterval, 
        tokenMaxLifetime, tokenRenewInterval, 3600000, store);
  }

  /**
   * 通过工厂方法创建历史服务器状态存储服务实例
   * @param conf 配置对象
   * @return 创建完成的状态存储服务实例
   */
  protected HistoryServerStateStoreService createStateStore(
      Configuration conf) {
    return HistoryServerStateStoreServiceFactory.getStore(conf);
  }

  /**
   * 在安全模式下完成JHS的Kerberos登录认证
   * @param conf 配置对象
   * @throws IOException 登录失败时抛出IO异常
   */
  protected void doSecureLogin(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(conf, JHAdminConfig.MR_HISTORY_KEYTAB,
        JHAdminConfig.MR_HISTORY_PRINCIPAL, socAddr.getHostName());
  }

  /**
   * 从配置中读取JHS绑定地址并解析
   *
   * @param conf 配置对象
   * @return 解析后的绑定地址
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(JHAdminConfig.MR_HISTORY_ADDRESS,
      JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS,
      JHAdminConfig.DEFAULT_MR_HISTORY_PORT);
  }

  @Override
  /**
   * 启动所有已注册的子服务
   */
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }
  
  @Override
  /**
   * 停止服务，关闭指标系统并停止所有子服务
   */
  protected void serviceStop() throws Exception {
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
  }

  @Private
  /**
   * 获取历史客户端服务实例
   * @return 历史客户端服务实例
   */
  public HistoryClientService getClientService() {
    return this.clientService;
  }

  /**
   * 启动作业历史服务器，完成初始化、注册关闭钩子并启动服务
   * @param args 启动参数
   * @return 启动完成的作业历史服务器实例
   */
  static JobHistoryServer launchJobHistoryServer(String[] args) {
    Thread.
        setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(JobHistoryServer.class, args, LOG);
    JobHistoryServer jobHistoryServer = null;
    try {
      jobHistoryServer = new JobHistoryServer();
      // 注册JVM关闭钩子，确保服务优雅关闭
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(jobHistoryServer),
          SHUTDOWN_HOOK_PRIORITY);
      // 加载配置，解析通用命令行参数
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      new GenericOptionsParser(conf, args);
      // 初始化并启动服务
      jobHistoryServer.init(conf);
      jobHistoryServer.start();
    } catch (Throwable t) {
      LOG.error("Error starting JobHistoryServer", t);
      ExitUtil.terminate(-1, "Error starting JobHistoryServer");
    }
    return jobHistoryServer;
  }

  /**
   * 作业历史服务器入口主方法
   * @param args 启动参数
   */
  public static void main(String[] args) {
    launchJobHistoryServer(args);
  }
}