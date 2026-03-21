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

import java.util.ArrayList;

import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;

/**
 * 文件说明：禁用节点黑名单功能的黑名单管理器实现
 * 
 * 不返回任何黑名单，也就是允许调度器将容器分配到任意节点，用于关闭黑名单功能的场景
 */
public class DisabledBlacklistManager implements BlacklistManager {

  // 预定义的空列表，复用减少对象创建
  private static final ArrayList<String> EMPTY_LIST = new ArrayList<String>();
  // 预定义的空黑名单请求，包含空的新增和移除列表
  private ResourceBlacklistRequest noBlacklist =
      ResourceBlacklistRequest.newInstance(EMPTY_LIST, EMPTY_LIST);

  /**
   * 添加节点到黑名单，禁用功能下不做任何处理
   */
  @Override
  public void addNode(String node) {
  }

  /**
   * 获取黑名单更新，始终返回空的黑名单更新
   */
  @Override
  public ResourceBlacklistRequest getBlacklistUpdates() {
    return noBlacklist;
  }

  /**
   * 刷新集群节点总数，禁用功能下不做任何处理
   */
  @Override
  public void refreshNodeHostCount(int nodeHostCount) {
    // Do nothing
  }
}