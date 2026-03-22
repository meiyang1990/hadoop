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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * NameNode状态信息的JMX管理接口，用于通过JMX暴露NameNode运行状态指标供监控系统采集。
 * 终端用户不应直接实现该接口，应通过JMX API访问其中暴露的状态信息。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface NameNodeStatusMXBean {

  /**
   * 获取当前NameNode的角色（HA场景下区分Active/Standby）。
   *
   * @return 当前NameNode的角色字符串
   */
  public String getNNRole();

  /**
   * 获取当前NameNode的运行状态。
   *
   * @return 当前NameNode的状态字符串
   */
  public String getState();

  /**
   * 获取当前NameNode的主机地址和端口，格式为冒号分隔。
   *
   * @return 冒号分隔的主机端口字符串
   */
  public String getHostAndPort();

  /**
   * 检查当前NameNode是否启用安全认证。
   *
   * @return true表示安全认证已启用，false表示未启用
   */
  public boolean isSecurityEnabled();

  /**
   * 获取最近一次HA状态切换的时间，单位为从纪元开始的毫秒数。
   *
   * @return 最近一次HA状态切换时间戳，单位毫秒
   */
  public long getLastHATransitionTime();

  /**
   * 获取具有未来世代戳的数据块总字节数。
   * @return 退出安全模式后可删除的字节总数
   */
  long getBytesWithFutureGenerationStamps();

  /**
   * 获取慢DataNodes报告，功能开启时返回JSON格式的慢节点信息。
   * @return JSON格式的慢节点报告
   */
  String getSlowPeersReport();


  /**
   * 获取集群中TopN慢磁盘信息，功能开启时返回JSON格式的慢磁盘报告。
   *
   * @return JSON格式的慢磁盘ID与延迟列表字符串
   */
  String getSlowDisksReport();
}