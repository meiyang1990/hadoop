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

package org.apache.hadoop.yarn.server.resourcemanager.blacklist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * YARN ResourceManager 简单节点黑名单管理器实现，维护应用程序分配失败的节点列表。
 * 当黑名单中节点占比低于阈值时，正常返回黑名单；超过阈值时清空黑名单，避免整个应用无法分配容器。
 */
public class SimpleBlacklistManager implements BlacklistManager {

  /** 集群中可用NodeManager节点总数 */
  private int numberOfNodeManagerHosts;
  /** 黑名单禁用阈值比例：黑名单节点占总节点比例超过该值时禁用黑名单 */
  private final double blacklistDisableFailureThreshold;
  /** 当前黑名单节点集合，存储失败节点地址 */
  private final Set<String> blacklistNodes = new HashSet<>();
  /** 空列表常量，用于返回空的黑名单更新请求 */
  private static final ArrayList<String> EMPTY_LIST = new ArrayList<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleBlacklistManager.class);

  /**
   * 构造简单黑名单管理器。
   * @param numberOfNodeManagerHosts 初始总NodeManager节点数
   * @param blacklistDisableFailureThreshold 黑名单禁用阈值比例
   */
  public SimpleBlacklistManager(int numberOfNodeManagerHosts,
      double blacklistDisableFailureThreshold) {
    this.numberOfNodeManagerHosts = numberOfNodeManagerHosts;
    this.blacklistDisableFailureThreshold = blacklistDisableFailureThreshold;
  }

  @Override
  public void addNode(String node) {
    blacklistNodes.add(node);
  }

  @Override
  public void refreshNodeHostCount(int nodeHostCount) {
    this.numberOfNodeManagerHosts = nodeHostCount;
  }

  @Override
  public ResourceBlacklistRequest getBlacklistUpdates() {
    ResourceBlacklistRequest ret;
    // 构造当前黑名单节点列表
    List<String> blacklist = new ArrayList<>(blacklistNodes);
    // 获取当前黑名单大小
    final int currentBlacklistSize = blacklist.size();
    // 计算绝对阈值：总节点数乘以阈值比例
    final double failureThreshold = this.blacklistDisableFailureThreshold *
        numberOfNodeManagerHosts;
    // 黑名单大小未超过阈值，正常返回新增黑名单
    if (currentBlacklistSize < failureThreshold) {
      LOG.debug("blacklist size {} is less than failure threshold ratio {}"
          + " out of total usable nodes {}", currentBlacklistSize,
          blacklistDisableFailureThreshold, numberOfNodeManagerHosts);
      ret = ResourceBlacklistRequest.newInstance(blacklist, EMPTY_LIST);
    } else {
      // 黑名单大小超过阈值，清空整个黑名单，避免应用无法分配容器
      LOG.warn("Ignoring Blacklists, blacklist size " + currentBlacklistSize
          + " is more than failure threshold ratio "
          + blacklistDisableFailureThreshold + " out of total usable nodes "
          + numberOfNodeManagerHosts);
      // TODO: After the threshold hits, we will keep sending a long list
      // every time a new AM is to be scheduled.
      ret = ResourceBlacklistRequest.newInstance(EMPTY_LIST, blacklist);
    }
    return ret;
  }
}