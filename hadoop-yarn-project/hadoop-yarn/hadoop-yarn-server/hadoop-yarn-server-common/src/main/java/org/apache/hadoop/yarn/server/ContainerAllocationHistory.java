// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件路径：org.apache.hadoop.yarn.server.ContainerAllocationHistory
 * 所属模块：YARN服务端公共组件
 * 核心职责：记录YARN ResourceManager的容器分配历史，统计聚合可放宽locality约束的分配延迟信息，用于联邦调度场景的分配分析
 * 记录Yarn RM的容器分配历史，并提供聚合统计信息
 */
public class ContainerAllocationHistory {
  private static final Logger LOG = LoggerFactory.getLogger(AMRMClientRelayer.class);

  private int maxEntryCount;

  // 存储可放宽locality约束的分配延迟历史：键为分配时间戳，值为分配延迟（从请求到完成的耗时）
  private Queue<Entry<Long, Long>> relaxableG = new LinkedList<>();

  /**
   * 构造函数，从配置中加载最大历史条目数
   * @param conf YARN配置对象
   */
  public ContainerAllocationHistory(Configuration conf) {
    this.maxEntryCount = conf.getInt(
        YarnConfiguration.FEDERATION_ALLOCATION_HISTORY_MAX_ENTRY,
        YarnConfiguration.DEFAULT_FEDERATION_ALLOCATION_HISTORY_MAX_ENTRY);
  }

  /**
   * 记录一次容器分配的历史条目
   *
   * @param container 本次分配得到的容器
   * @param requestSet 资源请求集合
   * @param fulfillTimeStamp 分配完成的时间戳
   * @param fulfillLatency 从请求发起到分配完成的耗时
   */
  public synchronized void addAllocationEntry(Container container,
      ResourceRequestSet requestSet, long fulfillTimeStamp, long fulfillLatency){
    // 只记录允许放宽ANY位置约束的分配请求
    if (!requestSet.isANYRelaxable()) {
      LOG.info("allocation history ignoring {}, relax locality is false", container);
      return;
    }
    // 添加新的分配记录到队列
    this.relaxableG.add(new AbstractMap.SimpleEntry<>(
        fulfillTimeStamp, fulfillLatency));
    // 超过最大存储条目数时，移除最早的一条记录
    if (this.relaxableG.size() > this.maxEntryCount) {
      this.relaxableG.remove();
    }
  }
}