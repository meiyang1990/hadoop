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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 文件功能：数据节点配置管理抽象接口，定义允许接入集群和需要退役/维护的节点管理规范
 *
 * 本接口抽象了数据节点准入与状态配置的管理方式，不同实现可以采用不同持久化方案存储配置：
 * 例如可以用单个JSON文件存储所有节点配置，也可以拆分文件分别存储运行中节点和待退役节点
 *
 * 核心职责：控制NameNode预期在集群中看到的数据节点范围，管理节点的准入、退役、升级域和维护状态配置
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class HostConfigManager implements Configurable {

  /**
   * 获取所有允许连接到NameNode的数据节点地址列表
   * @return 允许接入的数据节点地址可迭代对象
   */
  public abstract Iterable<InetSocketAddress> getIncludes();

  /**
   * 获取所有需要处于退役状态的数据节点地址列表
   * @return 需要退役的数据节点地址可迭代对象
   */
  public abstract Iterable<InetSocketAddress> getExcludes();

  /**
   * 检查指定数据节点是否被允许连接NameNode接入集群
   * @param dn 待检查数据节点的ID标识
   * @return true表示允许接入，false表示拒绝接入
   */
  public abstract boolean isIncluded(DatanodeID dn);

  /**
   * 检查指定数据节点是否需要执行退役流程
   * @param dn 待检查数据节点的ID标识
   * @return true表示需要退役，false表示不需要退役
   */
  public abstract boolean isExcluded(DatanodeID dn);

  /**
   * 重新加载数据节点配置文件，刷新内存中的配置信息
   * @throws IOException 刷新过程中IO异常时抛出
   */
  public abstract void refresh() throws IOException;

  /**
   * 获取指定数据节点所属的升级域标识
   * @param dn 目标数据节点的ID标识
   * @return 目标数据节点的升级域名称
   */
  public abstract String getUpgradeDomain(DatanodeID dn);

  /**
   * 获取指定数据节点维护模式的过期时间戳（毫秒）
   * @param dn 目标数据节点的ID标识
   * @return 维护模式过期时间戳，单位毫秒
   */
  public abstract long getMaintenanceExpirationTimeInMS(DatanodeID dn);
}