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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

/**
 * 文件说明：YARN资源管理器应用队列放置规则抽象基类，所有具体放置规则都需要继承此类
 * 核心职责：定义放置规则的统一接口，为PlacementManager提供规则执行的标准契约
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PlacementRule {

  /**
   * 设置规则配置，避免抽象类引入具体实现类的依赖
   * @param initArg 初始化配置参数
   */
  public void setConfig(Object initArg) {
    // 默认空实现，子类按需覆盖
  }

  /**
   * 获取当前规则的名称
   * @return 规则名称，默认返回规则类的全限定名
   */
  public String getName() {
    return this.getClass().getName();
  }

  /**
   * 使用调度器上下文初始化规则
   * @param scheduler 使用该规则的资源调度器
   * @return 初始化结果，true表示成功，false表示失败，结果由具体规则定义
   * @throws IOException 初始化过程中发生IO错误时抛出
   */
  public abstract boolean initialize(ResourceScheduler scheduler)
      throws IOException;

  /**
   * 根据应用上下文和用户信息计算应用应该放置到的目标队列
   * 
   * 返回非null表示已经确定目标队列，应用将被放置到该队列；
   * 返回null表示当前规则无法确定队列，将由PlacementManager执行下一条规则
   *
   * @param asc 应用提交时的上下文信息
   * @param user 提交应用的用户名
   * 
   * @throws YarnException 规则执行过程中发生错误时抛出
   * 
   * @return 包装了目标队列名的ApplicationPlacementContext，无法确定时返回null
   */
  public abstract ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException;


  /**
   * 带恢复标识的应用放置计算方法，支持应用恢复场景
   * 
   * 返回非null表示已经确定目标队列，应用将被放置到该队列；
   * 返回null表示当前规则无法确定队列，将由PlacementManager执行下一条规则
   *
   * @param asc 应用提交时的上下文信息
   * @param user 提交应用的用户名
   * @param recovery 标识本次提交是否是应用恢复
   *
   * @throws YarnException 规则执行过程中发生错误时抛出
   *
   * @return 包装了目标队列名的ApplicationPlacementContext，无法确定时返回null
   */
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user, boolean recovery)
      throws YarnException {
    return getPlacementForApp(asc, user);
  }
}