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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN 应用队列放置管理器，负责根据配置的放置规则将新提交的应用分配到对应队列。
 * 支持动态更新规则，通过读写锁保障规则访问的线程安全，采用顺序匹配规则找到第一个匹配项即停止。
 */
public class PlacementManager {  
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementManager.class);

  List<PlacementRule> rules;
  ReadLock readLock;
  WriteLock writeLock;

  /**
   * 构造放置管理器，初始化读写锁。
   */
  public PlacementManager() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * 更新放置规则列表，独占写锁保障更新安全。
   * @param rules 新的放置规则列表
   */
  public void updateRules(List<PlacementRule> rules) {
    writeLock.lock();
    try {
      this.rules = rules;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 根据提交信息为应用匹配放置队列，按顺序遍历规则，找到第一个匹配结果即返回。
   * @param asc 应用提交上下文，包含应用提交信息
   * @param user 提交应用的用户名
   * @param recovery 是否为应用恢复场景
   * @return 匹配到的放置上下文，包含目标队列信息，无匹配则返回null
   * @throws YarnException 规则匹配过程中抛出异常
   */
  public ApplicationPlacementContext placeApplication(
      ApplicationSubmissionContext asc, String user, boolean recovery)
      throws YarnException {
    readLock.lock();
    try {
      if (null == rules || rules.isEmpty()) {
        return null;
      }

      ApplicationPlacementContext placement = null;
      // 顺序遍历规则，匹配到即停止
      for (PlacementRule rule : rules) {
        placement = rule.getPlacementForApp(asc, user, recovery);
        if (placement != null) {
          break;
        }
      }

      return placement;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 非恢复场景下为应用匹配放置队列。
   * @param asc 应用提交上下文
   * @param user 提交应用的用户名
   * @return 匹配到的放置上下文，无匹配则返回null
   * @throws YarnException 规则匹配过程中抛出异常
   */
  public ApplicationPlacementContext placeApplication(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    return placeApplication(asc, user, false);
  }
  
  /**
   * 获取当前规则列表，仅用于测试。
   * @return 当前生效的放置规则列表
   */
  @VisibleForTesting
  public List<PlacementRule> getPlacementRules() {
    return rules;
  }
}