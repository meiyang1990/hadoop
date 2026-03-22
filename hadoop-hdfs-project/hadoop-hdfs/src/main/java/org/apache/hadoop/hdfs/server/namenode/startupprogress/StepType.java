// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：HDFS NameNode启动阶段步骤类型枚举，定义了NameNode启动过程中各个初始化阶段的类型
 *
 * 枚举，标识NameNode启动进度中每个启动步骤的具体类型
 */
@InterfaceAudience.Private
public enum StepType {
  /**
   * NameNode已进入安全模式，等待所有DataNode上报块信息
   */
  AWAITING_REPORTED_BLOCKS("AwaitingReportedBlocks", "awaiting reported blocks"),

  /**
   * NameNode正在加载维护 delegation 密钥相关数据
   */
  DELEGATION_KEYS("DelegationKeys", "delegation keys"),

  /**
   * NameNode正在加载维护 delegation 令牌相关数据
   */
  DELEGATION_TOKENS("DelegationTokens", "delegation tokens"),

  /**
   * NameNode正在加载文件系统inode元数据
   */
  INODES("Inodes", "inodes"),

  /**
   * NameNode正在加载缓存池配置信息
   */
  CACHE_POOLS("CachePools", "cache pools"),

  /**
   * NameNode正在加载缓存条目信息
   */
  CACHE_ENTRIES("CacheEntries", "cache entries"),

  /**
   * NameNode正在加载纠删码策略配置
   */
  ERASURE_CODING_POLICIES("ErasureCodingPolicies", "erasure coding policies");

  private final String name, description;

  /**
   * 枚举构造方法，初始化步骤类型的名称和描述
   * 
   * @param name 步骤类型名称
   * @param description 步骤类型描述
   */
  private StepType(String name, String description) {
    this.name = name;
    this.description = description;
  }

  /**
   * 获取当前启动步骤类型的描述文本
   * 
   * @return 步骤类型描述
   */
  public String getDescription() {
    return description;
  }

  /**
   * 获取当前启动步骤类型的名称
   * 
   * @return 步骤类型名称
   */
  public String getName() {
    return name;
  }
}