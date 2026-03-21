// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  fair调度器配置转容量调度器配置结果校验器，通过实际启动容量调度器实例来验证转换后的配置正确性。
 *
 */
public class ConvertedConfigValidator {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConvertedConfigValidator.class);

  /**
   * 验证转换后的容量调度器配置，通过启动容量调度器实例确认配置合法可用。
   * @param outputDir 转换后配置文件所在的输出目录
   * @throws Exception 配置验证失败时抛出异常
   */
  public void validateConvertedConfig(String outputDir)
      throws Exception {
    // 清除旧队列指标，避免影响新启动的调度器
    QueueMetrics.clearQueueMetrics();
    // 构造转换后的容量调度器配置文件路径
    Path configPath = new Path(outputDir, "capacity-scheduler.xml");

    // 创建容量调度器配置对象，加载转换后的配置
    CapacitySchedulerConfiguration csConfig =
        new CapacitySchedulerConfiguration(
            new Configuration(false), false);
    csConfig.addResource(configPath);

    // 构造转换后的yarn-site配置文件路径
    Path convertedSiteConfigPath = new Path(outputDir, "yarn-site.xml");
    // 创建Yarn配置对象，加载转换后的yarn-site配置
    Configuration siteConf = new YarnConfiguration(
        new Configuration(false));
    siteConf.addResource(convertedSiteConfigPath);

    // 构建RM上下文对象，用于初始化容量调度器
    RMContextImpl rmContext = new RMContextImpl();
    // 设置配置存储路径为输出目录
    siteConf.set(YarnConfiguration.FS_BASED_RM_CONF_STORE, outputDir);
    // 初始化文件系统配置提供者
    ConfigurationProvider provider = new FileSystemBasedConfigurationProvider();
    provider.init(siteConf);
    rmContext.setConfigurationProvider(provider);
    // 初始化节点标签管理器并设置到RM上下文
    RMNodeLabelsManager mgr = new RMNodeLabelsManager();
    mgr.init(siteConf);
    rmContext.setNodeLabelManager(mgr);

    // 尝试启动容量调度器验证配置正确性
    try (CapacityScheduler cs = new CapacityScheduler()) {
      cs.setConf(siteConf);
      cs.setRMContext(rmContext);
      cs.serviceInit(csConfig);
      cs.serviceStart();
      LOG.info("Capacity scheduler was successfully started");
      cs.serviceStop();
    } catch (Exception e) {
      LOG.error("Could not start Capacity Scheduler", e);
      throw new VerificationException(
          "Verification of converted configuration failed", e);
    }
  }
}