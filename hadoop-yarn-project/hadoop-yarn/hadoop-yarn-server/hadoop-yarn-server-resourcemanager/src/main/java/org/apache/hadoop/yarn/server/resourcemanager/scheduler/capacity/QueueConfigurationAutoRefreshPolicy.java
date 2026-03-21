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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;

/**
 * 容量调度器队列配置自动刷新策略，实现调度计划编辑策略接口，
 * 定期检查配置文件变更，自动触发队列配置重新加载。
 */
public class QueueConfigurationAutoRefreshPolicy
    implements SchedulingEditPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueueConfigurationAutoRefreshPolicy.class);

  private Clock clock;

  // 持有RM其他组件的引用
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;
  private RMNodeLabelsManager nlm;

  private long monitoringInterval;
  private long lastModified;

  // 上次尝试重新加载队列配置的时间（包含成功和失败场景）
  private long lastReloadAttempt;
  private boolean lastReloadAttemptFailed = false;

  // 容量调度器队列配置文件路径
  private Path allocCsFile;
  private FileSystem fs;

  /**
   * 默认构造函数，由容量调度器配置反射实例化。
   */
  public QueueConfigurationAutoRefreshPolicy() {
    clock = new MonotonicClock();
  }

  @Override
  /**
   * 初始化自动刷新策略，绑定RM上下文和调度器，加载配置参数。
   */
  public void init(final Configuration config, final RMContext context,
                   final ResourceScheduler sched) {
    LOG.info("Queue auto refresh Policy monitor: {}" + this.
        getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    // 校验调度器类型必须是CapacityScheduler
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    clock = scheduler.getClock();

    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();

    CapacitySchedulerConfiguration csConfig = scheduler.getConfiguration();

    // 从配置中加载监控检查间隔，使用默认值兜底
    monitoringInterval = csConfig.getLong(
        CapacitySchedulerConfiguration.QUEUE_AUTO_REFRESH_MONITORING_INTERVAL,
        CapacitySchedulerConfiguration.
            DEFAULT_QUEUE_AUTO_REFRESH_MONITORING_INTERVAL);
  }


  @Override
  /**
   * 执行调度编辑逻辑，检查配置文件变更，满足条件则触发队列刷新。
   */
  public void editSchedule() {
    long startTs = clock.getTime();

    try {

      // 根据配置提供者类型，确定配置文件路径：支持文件系统提供和本地类路径两种方式
      if (rmContext.getYarnConfiguration().
          get(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS).
          equals(FileSystemBasedConfigurationProvider
              .class.getCanonicalName())) {
        allocCsFile = new Path(rmContext.getYarnConfiguration().
            get(YarnConfiguration.FS_BASED_RM_CONF_STORE),
            YarnConfiguration.CS_CONFIGURATION_FILE);
      } else {
        allocCsFile =  new Path(rmContext.getYarnConfiguration()
            .getClassLoader().getResource("").toString(),
            YarnConfiguration.CS_CONFIGURATION_FILE);
      }

      // 获取配置文件所在文件系统，获取配置文件最后修改时间
      fs =  allocCsFile.getFileSystem(rmContext.getYarnConfiguration());

      lastModified =
          fs.getFileStatus(allocCsFile).getModificationTime();

      long time = clock.getTime();

      // 判断是否满足刷新条件：配置文件已修改 且 距离上次尝试已超过监控间隔
      if (lastModified > lastReloadAttempt &&
          time > lastReloadAttempt + monitoringInterval) {
        try {
          // 调用RMAdmin接口触发队列刷新
          rmContext.getRMAdminService().refreshQueues();
          LOG.info("Queue auto refresh completed successfully");
          lastReloadAttempt = clock.getTime();
          lastReloadAttemptFailed = false;
        } catch (IOException | YarnException e) {
          LOG.error("Can't refresh queue: " + e);
          // 首次失败才打印错误日志，避免重复刷屏
          if (!lastReloadAttemptFailed) {
            LOG.error("Failed to reload capacity scheduler config file - " +
                "will use existing conf. Message: {}", e.getMessage());
          }
          lastReloadAttempt = clock.getTime();
          lastReloadAttemptFailed = true;
        }

      } else if (lastModified == 0L) {
        // 获取文件修改时间返回0，提示文件不存在或异常
        if (!lastReloadAttemptFailed) {
          LOG.warn("Failed to reload capacity scheduler config file because" +
              " last modified returned 0. File exists: "
              + fs.exists(allocCsFile));
        }
        lastReloadAttemptFailed = true;
      }

    } catch (IOException e) {
      LOG.error("Can't get file status for refresh : " + e);
    }

    // 调试日志打印本次检查耗时
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  @VisibleForTesting
  long getLastReloadAttempt() {
    return lastReloadAttempt;
  }

  @VisibleForTesting
  long getLastModified() {
    return lastModified;
  }

  @VisibleForTesting
  Clock getClock() {
    return clock;
  }

  @VisibleForTesting
  boolean getLastReloadAttemptFailed() {
    return  lastReloadAttemptFailed;
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return QueueConfigurationAutoRefreshPolicy.class.getCanonicalName();
  }
}