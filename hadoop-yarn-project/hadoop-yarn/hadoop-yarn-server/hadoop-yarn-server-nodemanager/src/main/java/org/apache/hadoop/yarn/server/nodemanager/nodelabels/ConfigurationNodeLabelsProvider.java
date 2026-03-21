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

import java.io.IOException;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 从配置文件定时读取节点标签，为NodeManager提供节点标签能力
 * 持续监控配置文件变更，动态更新当前节点的标签信息
 */
public class ConfigurationNodeLabelsProvider extends NodeLabelsProvider {

  private static final Logger LOG =
       LoggerFactory.getLogger(ConfigurationNodeLabelsProvider.class);

  /**
   * 构造基于配置文件的节点标签提供者
   */
  public ConfigurationNodeLabelsProvider() {
    super("Configuration Based NodeLabels Provider");
  }

  @Override
  /**
   * 服务初始化，从配置读取标签更新间隔，设置定时器参数
   */
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置获取配置检查间隔，使用默认值兜底
    long taskInterval = conf.getLong(
        YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS);
    this.setIntervalTime(taskInterval);
    super.serviceInit(conf);
  }

  /**
   * 从配置文件读取并更新当前节点标签
   * @param conf 配置对象
   * @throws IOException 读取配置异常
   */
  private void updateNodeLabelsFromConfig(Configuration conf)
      throws IOException {
    // 从配置获取当前节点所属分区标签
    String configuredNodePartition =
        conf.get(YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_PARTITION, null);
    // 转换标签格式并更新节点标签描述符
    setDescriptors(convertToNodeLabelSet(configuredNodePartition));
  }

  /**
   * 定时检查配置文件变更的定时器任务
   */
  private class ConfigurationMonitorTimerTask extends TimerTask {
    @Override
    public void run() {
      try {
        // 重新加载Yarn配置，更新节点标签
        updateNodeLabelsFromConfig(new YarnConfiguration());
      } catch (Exception e) {
        LOG.error("Failed to update node Labels from configuration.xml ", e);
      }
    }
  }

  @Override
  /**
   * 创建定时检查配置的定时器任务实例
   * @return 配置监控定时器任务
   */
  public TimerTask createTimerTask() {
    return new ConfigurationMonitorTimerTask();
  }

  @Override
  /**
   * 清理资源，该实现无需额外清理
   */
  protected void cleanUp() throws Exception {
    //No cleanup Req!
  }
}