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
package org.apache.hadoop.mapreduce;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件: SharedCacheConfig.java
 * 所属模块: hadoop-mapreduce-client-core
 * 核心职责: 解析MapReduce共享缓存相关配置，判断各类资源是否启用共享缓存
 * 
 * 共享缓存是YARN提供的能力，用于在节点级别缓存公共资源，避免多个任务重复下载相同资源，
 * 减少网络IO和磁盘占用，提升作业执行效率。此类用于根据配置确定各类资源是否启用共享缓存。
 */
@Private
@Unstable
public class SharedCacheConfig {
  // 日志记录器
  protected static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheConfig.class);

  private boolean sharedCacheFilesEnabled = false;
  private boolean sharedCacheLibjarsEnabled = false;
  private boolean sharedCacheArchivesEnabled = false;
  private boolean sharedCacheJobjarEnabled = false;

  /**
   * 从配置对象初始化共享缓存配置，根据配置判断各类资源是否启用共享缓存
   * @param conf Hadoop配置对象
   */
  public void init(Configuration conf) {
    // 仅在YARN运行框架下支持共享缓存，非YARN模式直接返回
    if (!MRConfig.YARN_FRAMEWORK_NAME.equals(conf.get(
        MRConfig.FRAMEWORK_NAME))) {
      // Shared cache is only valid if the job runs on yarn
      return;
    }

    // YARN全局共享缓存未开启，直接返回
    if(!conf.getBoolean(YarnConfiguration.SHARED_CACHE_ENABLED,
        YarnConfiguration.DEFAULT_SHARED_CACHE_ENABLED)) {
      return;
    }

    // 解析共享缓存模式配置，去除空格分割得到模式列表
    Collection<String> configs = StringUtils.getTrimmedStringCollection(
        conf.get(MRJobConfig.SHARED_CACHE_MODE,
            MRJobConfig.SHARED_CACHE_MODE_DEFAULT));
    // 根据配置标记对应资源类型是否启用共享缓存
    if (configs.contains("files")) {
      this.sharedCacheFilesEnabled = true;
    }
    if (configs.contains("libjars")) {
      this.sharedCacheLibjarsEnabled = true;
    }
    if (configs.contains("archives")) {
      this.sharedCacheArchivesEnabled = true;
    }
    if (configs.contains("jobjar")) {
      this.sharedCacheJobjarEnabled = true;
    }
    // 全局开启配置：所有资源类型都启用共享缓存
    if (configs.contains("enabled")) {
      this.sharedCacheFilesEnabled = true;
      this.sharedCacheLibjarsEnabled = true;
      this.sharedCacheArchivesEnabled = true;
      this.sharedCacheJobjarEnabled = true;
    }
    // 全局关闭配置：所有资源类型都禁用共享缓存
    if (configs.contains("disabled")) {
      this.sharedCacheFilesEnabled = false;
      this.sharedCacheLibjarsEnabled = false;
      this.sharedCacheArchivesEnabled = false;
      this.sharedCacheJobjarEnabled = false;
    }
  }

  /**
   * 获取普通文件是否启用共享缓存
   * @return true表示启用，false表示不启用
   */
  public boolean isSharedCacheFilesEnabled() {
    return sharedCacheFilesEnabled;
  }

  /**
   * 获取依赖Jar包是否启用共享缓存
   * @return true表示启用，false表示不启用
   */
  public boolean isSharedCacheLibjarsEnabled() {
    return sharedCacheLibjarsEnabled;
  }

  /**
   * 获取归档文件是否启用共享缓存
   * @return true表示启用，false表示不启用
   */
  public boolean isSharedCacheArchivesEnabled() {
    return sharedCacheArchivesEnabled;
  }

  /**
   * 获取作业Jar包是否启用共享缓存
   * @return true表示启用，false表示不启用
   */
  public boolean isSharedCacheJobjarEnabled() {
    return sharedCacheJobjarEnabled;
  }

  /**
   * 判断是否有任意一种资源类型启用了共享缓存
   * @return true表示至少一种资源启用共享缓存，false表示全部未启用
   */
  public boolean isSharedCacheEnabled() {
    return (sharedCacheFilesEnabled || sharedCacheLibjarsEnabled ||
        sharedCacheArchivesEnabled || sharedCacheJobjarEnabled);
  }
}