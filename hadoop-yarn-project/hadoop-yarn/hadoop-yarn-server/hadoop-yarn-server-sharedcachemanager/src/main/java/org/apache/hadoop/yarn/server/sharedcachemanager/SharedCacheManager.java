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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.webapp.SCMWebServer;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN共享缓存管理器服务，负责维护共享缓存元数据，处理资源申请释放、客户端RPC调用、管理命令，
 * 将元数据持久化到后端存储，并定期清理过期缓存条目。
 */
@Private
@Unstable
public class SharedCacheManager extends CompositeService {
  /**
   * 共享缓存管理器关闭钩子的优先级
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheManager.class);

  // 共享缓存元数据存储对象
  private SCMStore store;

  public SharedCacheManager() {
    super("SharedCacheManager");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    // 创建并添加元数据存储服务
    this.store = createSCMStoreService(conf);
    addService(store);

    // 创建并添加过期缓存清理服务
    CleanerService cs = createCleanerService(store);
    addService(cs);

    // 创建并添加NodeManager缓存上传协议服务
    SharedCacheUploaderService nms =
        createNMCacheUploaderSCMProtocolService(store);
    addService(nms);

    // 创建并添加客户端协议服务
    ClientProtocolService cps = createClientProtocolService(store);
    addService(cps);

    // 创建并添加管理员协议服务
    SCMAdminProtocolService saps = createSCMAdminProtocolService(cs);
    addService(saps);

    // 创建并添加Web UI服务
    SCMWebServer webUI = createSCMWebServer(this);
    addService(webUI);

    // 初始化metrics系统
    DefaultMetricsSystem.initialize("SharedCacheManager");
    JvmMetrics.initSingleton("SharedCacheManager", null);

    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  // 根据配置创建SCM存储服务实例
  private static SCMStore createSCMStoreService(Configuration conf) {
    Class<? extends SCMStore> defaultStoreClass;
    try {
      // 加载默认存储实现类
      defaultStoreClass =
          (Class<? extends SCMStore>) Class
              .forName(YarnConfiguration.DEFAULT_SCM_STORE_CLASS);
    } catch (Exception e) {
      throw new YarnRuntimeException("Invalid default scm store class"
          + YarnConfiguration.DEFAULT_SCM_STORE_CLASS, e);
    }

    // 根据配置创建存储实例，使用默认类作为后备
    SCMStore store =
        ReflectionUtils.newInstance(conf.getClass(
            YarnConfiguration.SCM_STORE_CLASS,
            defaultStoreClass, SCMStore.class), conf);
    return store;
  }

  // 创建过期缓存清理服务
  private CleanerService createCleanerService(SCMStore store) {
    return new CleanerService(store);
  }

  // 创建NodeManager缓存上传协议服务
  private SharedCacheUploaderService
      createNMCacheUploaderSCMProtocolService(SCMStore store) {
    return new SharedCacheUploaderService(store);
  }

  // 创建客户端协议服务
  private ClientProtocolService createClientProtocolService(SCMStore store) {
    return new ClientProtocolService(store);
  }

  // 创建管理员协议服务
  private SCMAdminProtocolService createSCMAdminProtocolService(
      CleanerService cleanerService) {
    return new SCMAdminProtocolService(cleanerService);
  }

  // 创建Web UI服务
  private SCMWebServer createSCMWebServer(SharedCacheManager scm) {
    return new SCMWebServer(scm);
  }

  @Override
  protected void serviceStop() throws Exception {
    // 关闭metrics系统
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
  }

  /**
   * 仅用于测试，获取元数据存储对象
   */
  @VisibleForTesting
  SCMStore getSCMStore() {
    return this.store;
  }

  /**
   * 共享缓存管理器启动入口
   */
  public static void main(String[] args) {
    // 设置默认未捕获异常处理器
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    // 打印启动日志信息
    StringUtils.startupShutdownMessage(SharedCacheManager.class, args, LOG);
    try {
      // 加载YARN配置
      Configuration conf = new YarnConfiguration();
      // 创建共享缓存管理器实例
      SharedCacheManager sharedCacheManager = new SharedCacheManager();
      // 注册关闭钩子，确保服务正常退出
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(sharedCacheManager),
          SHUTDOWN_HOOK_PRIORITY);
      // 初始化并启动服务
      sharedCacheManager.init(conf);
      sharedCacheManager.start();
    } catch (Throwable t) {
      LOG.error("Error starting SharedCacheManager", t);
      System.exit(-1);
    }
  }
}