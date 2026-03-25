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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;

// 这个文件已经全部加上中文注释
// 应用程序状态检查器抽象类，用于判断应用是否仍在运行，供清理服务决定是否可安全删除缓存条目
/**
 * YARN共享缓存应用状态检查抽象接口，供缓存清理服务判断缓存条目是否可以安全删除。
 * 核心作用是提供应用运行状态查询能力，确保只清理没有活跃应用依赖的缓存资源。
 */
@Private
@Evolving
public abstract class AppChecker extends CompositeService {

  /** 构造函数，使用默认服务名称初始化AppChecker */
  public AppChecker() {
    super("AppChecker");
  }

  /**
   * 构造函数，使用自定义服务名称初始化AppChecker
   * @param name 服务名称
   */
  public AppChecker(String name) {
    super(name);
  }

  /**
   * 判断指定应用是否处于活跃运行状态
   * 
   * @param id 待检查的应用ID
   * @return true 如果应用存在且未完成；false 如果应用不存在或已完成
   * @throws YarnException 查询应用状态过程中发生错误时抛出
   */
  @Private
  public abstract boolean isApplicationActive(ApplicationId id)
      throws YarnException;

  /**
   * 获取当前集群中所有活跃运行的应用列表
   * 
   * @return 活跃应用ID集合，无活跃应用时返回空集合
   * @throws YarnException 获取活跃应用列表过程中发生错误时抛出
   */
  @Private
  public abstract Collection<ApplicationId> getActiveApplications()
      throws YarnException;
}