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
package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import java.util.Collections;
import java.util.Map;

/**
 * 文件：联邦全局策略抽象基类
 * 所属模块：YARN全局策略生成器
 * 核心职责：定义可插拔的全局策略生成接口，供策略生成器扩展实现不同的联邦路由策略生成逻辑
 * 作用：为全局策略生成器提供统一的扩展点，允许自定义根据集群状态生成联邦路由策略的逻辑
 */
public abstract class GlobalPolicy implements Configurable {

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * 注册需要从各子集群RM端点获取的信息类型与对应路径
   * 框架统一查询后提供给策略，避免每个队列重复查询相同端点优化性能
   * @return 映射表：key为需要获取的对象类型，value为RM端点路径
   */
  protected Map<Class<?>, String> registerPaths() {
    // 默认不注册任何端点
    return Collections.emptyMap();
  }

  /**
   * 根据当前集群状态更新或创建队列的联邦路由策略
   * 该方法定义了具体策略生成器的核心行为，由子类实现
   *
   * @param queueName   当前处理的队列名称
   * @param clusterInfo 子集群信息映射表：key为子集群ID，value为该子集群对应的各类度量信息对象映射
   * @param manager     队列已有的联邦策略管理器，若为null则需要新建策略管理器
   * @return 更新或新建后的联邦策略管理器，将被保存到联邦状态存储
   */
  protected abstract FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo,
      FederationPolicyManager manager);

}