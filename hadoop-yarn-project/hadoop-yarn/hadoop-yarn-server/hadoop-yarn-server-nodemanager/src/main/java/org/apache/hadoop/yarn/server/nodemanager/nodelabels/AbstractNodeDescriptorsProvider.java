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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Collections;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * 文件：AbstractNodeDescriptorsProvider.java
 * 所属模块：YARN NodeManager 节点标签模块
 * 核心职责：提供节点描述符提供者的抽象基类，封装了定时拉取节点描述符的通用逻辑，子类只需实现具体拉取逻辑即可
 * 
 * Provides base Implementation of NodeDescriptorsProvider with Timer and
 * expects subclass to provide TimerTask which can fetch node descriptors.
 */
public abstract class AbstractNodeDescriptorsProvider<T>
    extends AbstractService implements NodeDescriptorsProvider<T> {
  // 禁用定时拉取的标识值
  public static final long DISABLE_NODE_DESCRIPTORS_PROVIDER_FETCH_TIMER = -1;

  // 定时拉取节点描述符的间隔时间，-1表示仅执行一次拉取，子类可通过配置覆盖该值
  private long intervalTime = -1;

  // 用于调度定时拉取任务的定时器
  private Timer scheduler;

  // 读写锁，保证节点描述符并发读写的线程安全
  protected Lock readLock = null;
  protected Lock writeLock = null;

  // 子类实现的定时拉取任务实例
  protected TimerTask timerTask;

  // 存储当前节点描述符集合，使用不可修改集合保证线程安全
  private Set<T> nodeDescriptors = Collections
      .unmodifiableSet(new HashSet<>(0));

  public AbstractNodeDescriptorsProvider(String name) {
    super(name);
  }

  /**
   * 获取定时拉取间隔时间
   * @return 间隔时间（毫秒）
   */
  public long getIntervalTime() {
    return intervalTime;
  }

  /**
   * 设置定时拉取间隔时间
   * @param intervalMS 间隔时间（毫秒）
   */
  public void setIntervalTime(long intervalMS) {
    this.intervalTime = intervalMS;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 初始化读写锁，用于节点描述符的并发访问控制
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 创建子类实现的定时拉取任务
    timerTask = createTimerTask();
    // 启动时先执行一次拉取
    timerTask.run();
    long taskInterval = getIntervalTime();
    // 如果未禁用定时拉取，则启动周期调度
    if (taskInterval != DISABLE_NODE_DESCRIPTORS_PROVIDER_FETCH_TIMER) {
      scheduler =
          new Timer("DistributedNodeDescriptorsRunner-Timer", true);
      // 按配置的间隔时间周期性调度拉取任务，非法参数会由Timer API自行处理
      scheduler.schedule(timerTask, taskInterval, taskInterval);
    }
    super.serviceStart();
  }

  /**
   * 服务停止时终止定时器，清理资源
   * @throws Exception
   */
  @Override
  protected void serviceStop() throws Exception {
    if (scheduler != null) {
      scheduler.cancel();
    }
    cleanUp();
    super.serviceStop();
  }

  /**
   * 留给子类实现的清理方法，用于子类自定义资源清理
   * @throws Exception
   */
  protected abstract void cleanUp() throws Exception ;

  /**
   * 获取当前节点描述符集合（线程安全读操作）
   * @return 当前节点描述符集合
   */
  @Override
  public Set<T> getDescriptors() {
    readLock.lock();
    try {
      return this.nodeDescriptors;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 更新节点描述符集合（线程安全写操作）
   * @param descriptorsSet 新的节点描述符集合
   */
  @Override
  public void setDescriptors(Set<T> descriptorsSet) {
    writeLock.lock();
    try {
      this.nodeDescriptors = descriptorsSet;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 验证节点描述符拉取脚本配置是否合法：脚本路径非空、文件存在且有可执行权限
   * @param scriptPath 脚本文件路径
   * @throws IOException 配置不合法时抛出异常
   */
  protected void verifyConfiguredScript(String scriptPath)
      throws IOException {
    boolean invalidConfiguration;
    if (scriptPath == null
        || scriptPath.trim().isEmpty()) {
      invalidConfiguration = true;
    } else {
      File f = new File(scriptPath);
      invalidConfiguration = !f.exists() || !FileUtil.canExecute(f);
    }
    if (invalidConfiguration) {
      throw new IOException(
          "Node descriptors provider script \"" + scriptPath
              + "\" is not configured properly. Please check whether"
              + " the script path exists, owner and the access rights"
              + " are suitable for NM process to execute it");
    }
  }

  /**
   * 将分区标签字符串转换为NodeLabel集合
   * @param partitionNodeLabel 分区标签字符串
   * @return 包含该标签的NodeLabel集合
   */
  static Set<NodeLabel> convertToNodeLabelSet(String partitionNodeLabel) {
    if (null == partitionNodeLabel) {
      return null;
    }
    Set<NodeLabel> labels = new HashSet<NodeLabel>();
    labels.add(NodeLabel.newInstance(partitionNodeLabel));
    return labels;
  }

  /**
   * 仅用于测试，获取当前定时任务实例
   * @return 定时任务实例
   */
  TimerTask getTimerTask() {
    return timerTask;
  }

  @VisibleForTesting
  public Timer getScheduler() {
    return this.scheduler;
  }

  /**
   * 抽象方法，由子类创建具体的定时拉取任务实例，任务负责更新节点描述符
   * @return 定时拉取任务实例
   */
  public abstract TimerTask createTimerTask();
}