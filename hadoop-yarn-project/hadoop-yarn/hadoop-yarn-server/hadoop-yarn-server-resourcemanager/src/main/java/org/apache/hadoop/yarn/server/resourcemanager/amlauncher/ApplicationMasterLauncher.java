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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

/**
 * ApplicationMaster启动器，负责处理YARN应用尝试的启动和清理事件
 * 接收AMLauncherEvent事件，异步分发到线程池执行启动/清理操作
 */
public class ApplicationMasterLauncher extends AbstractService implements
    EventHandler<AMLauncherEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ApplicationMasterLauncher.class);
  // 启动任务线程池
  private ThreadPoolExecutor launcherPool;
  // 事件处理主线程，从队列取出事件分发到线程池
  private LauncherThread launcherHandlingThread;
  
  // 存放待处理的ApplicationMaster事件队列
  private final BlockingQueue<Runnable> masterEvents
    = new LinkedBlockingQueue<Runnable>();
  
  // RM上下文，持有ResourceManager全局状态
  protected final RMContext context;
  
  /**
   * 构造ApplicationMaster启动器
   * @param context RM上下文
   */
  public ApplicationMasterLauncher(RMContext context) {
    super(ApplicationMasterLauncher.class.getName());
    this.context = context;
    this.launcherHandlingThread = new LauncherThread();
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取启动器线程数，使用默认值如果未配置
    int threadCount = conf.getInt(
        YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);
    // 构建命名线程工厂，方便日志定位
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("ApplicationMasterLauncher #%d")
        .build();
    // 初始化固定大小线程池
    launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
    launcherPool.setThreadFactory(tf);

    // 创建新配置对象，修改连接NodeManager的最大重试次数
    Configuration newConf = new YarnConfiguration(conf);
    newConf.setInt(CommonConfigurationKeysPublic.
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
            YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES));
    setConfig(newConf);
    super.serviceInit(newConf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 启动事件处理线程
    launcherHandlingThread.start();
    super.serviceStart();
  }
  
  /**
   * 创建ApplicationMaster启动/清理任务Runnable
   * @param application 应用尝试
   * @param event 事件类型（启动/清理）
   * @return 可执行任务
   */
  protected Runnable createRunnableLauncher(RMAppAttempt application, 
      AMLauncherEventType event) {
    Runnable launcher =
        new AMLauncher(context, application, event, getConfig());
    return launcher;
  }
  
  /**
   * 提交ApplicationMaster启动任务到事件队列
   * @param application 待启动的应用尝试
   */
  private void launch(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, 
        AMLauncherEventType.LAUNCH);
    masterEvents.add(launcher);
  }
  

  @Override
  protected void serviceStop() throws Exception {
    // 中断事件处理线程
    launcherHandlingThread.interrupt();
    try {
      // 等待事件处理线程退出
      launcherHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info(launcherHandlingThread.getName() + " interrupted during join ", 
          ie);    
    }
    // 关闭线程池
    launcherPool.shutdown();
  }

  /**
   * 事件处理主线程，持续从队列取出任务提交到线程池执行
   */
  private class LauncherThread extends SubjectInheritingThread {
    
    public LauncherThread() {
      super("ApplicationMaster Launcher");
    }

    @Override
    public void work() {
      while (!this.isInterrupted()) {
        Runnable toLaunch;
        try {
          // 阻塞取出待执行任务
          toLaunch = masterEvents.take();
          // 提交到线程池执行
          launcherPool.execute(toLaunch);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }
  }    

  /**
   * 提交ApplicationMaster清理任务到事件队列
   * @param application 待清理的应用尝试
   */
  private void cleanup(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
    masterEvents.add(launcher);
  } 
  
  @Override
  public synchronized void  handle(AMLauncherEvent appEvent) {
    AMLauncherEventType event = appEvent.getType();
    RMAppAttempt application = appEvent.getAppAttempt();
    // 根据事件类型分发处理
    switch (event) {
    case LAUNCH:
      launch(application);
      break;
    case CLEANUP:
      cleanup(application);
      break;
    default:
      break;
    }
  }
}