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
package org.apache.hadoop.hdfs.server.sps.metrics;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.sps.ItemInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.metrics2.util.MBeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.util.HashSet;

/**
 * 文件说明：外部存储策略满足器（ExternalSPS）的JMX指标暴露实现类
 * 核心职责：将ExternalSPS的运行状态指标通过JMX MBean暴露，供监控系统采集查看
 */
/**
 * Expose the ExternalSPS metrics.
 */
public class ExternalSPSBeanMetrics implements ExternalSPSMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(ExternalSPSBeanMetrics.class);

  /**
   * ExternalSPS JMX MBean对象名
   */
  private ObjectName externalSPSBeanName;
  /**
   * 持有存储策略满足器实例引用，用于获取运行指标
   */
  private StoragePolicySatisfier storagePolicySatisfier;

  /**
   * 构造函数，注册ExternalSPS的JMX MBean到MBean服务器
   * @param sps 存储策略满足器实例
   */
  public ExternalSPSBeanMetrics(StoragePolicySatisfier sps) {
    try {
      this.storagePolicySatisfier = sps;
      StandardMBean bean = new StandardMBean(this, ExternalSPSMXBean.class);
      this.externalSPSBeanName = MBeans.register("ExternalSPS", "ExternalSPS", bean);
      LOG.info("Registered ExternalSPS MBean: {}", this.externalSPSBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad externalSPS MBean setup", e);
    }
  }

  /**
   * 注销JMX MBean，释放资源，关闭指标暴露
   */
  public void close() {
    if (externalSPSBeanName != null) {
      MBeans.unregister(externalSPSBeanName);
      externalSPSBeanName = null;
    }
  }

  @Override
  public int getProcessingQueueSize() {
    return storagePolicySatisfier.processingQueueSize();
  }

  /**
   * 测试专用方法，向处理队列添加测试数据，用于指标验证
   */
  @VisibleForTesting
  public void updateProcessingQueueSize() {
    storagePolicySatisfier.getStorageMovementQueue()
        .add(new ItemInfo(0, 1, 1));
  }

  @Override
  public int getMovementFinishedBlocksCount() {
    return storagePolicySatisfier.getAttemptedItemsMonitor().getMovementFinishedBlocksCount();
  }

  /**
   * 测试专用方法，向已完成移动块集合添加测试数据，用于指标验证
   */
  @VisibleForTesting
  public void updateMovementFinishedBlocksCount() {
    storagePolicySatisfier.getAttemptedItemsMonitor().getMovementFinishedBlocks()
        .add(new Block(1));
  }

  @Override
  public int getAttemptedItemsCount() {
    return storagePolicySatisfier.getAttemptedItemsMonitor().getAttemptedItemsCount();
  }

  /**
   * 测试专用方法，向已尝试移动项集合添加测试数据，用于指标验证
   */
  @VisibleForTesting
  public void updateAttemptedItemsCount() {
    storagePolicySatisfier.getAttemptedItemsMonitor().getStorageMovementAttemptedItems()
        .add(new StoragePolicySatisfier.AttemptedItemInfo(0, 1,
            1, new HashSet<>(), 1));
  }
}