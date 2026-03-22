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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Map;

/**
 * DataNode节点信息的JMX管理接口。
 * 终端用户不应该自行实现该接口，应通过JMX API获取对应监控信息。
 * 该接口为Hadoop HDFS DataNode提供JMX监控能力，暴露节点运行时指标供监控系统采集。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface DataNodeMXBean {
  
  /**
   * 获取Hadoop版本号。
   * 
   * @return Hadoop版本字符串
   */
  public String getVersion();

  /**
   * 获取DataNode运行的软件版本。
   *
   * @return 代表软件版本的字符串
   */
  public String getSoftwareVersion();

  /**
   * 获取DataNode的RPC服务端口。
   * 
   * @return RPC端口字符串
   */
  public String getRpcPort();
  
  /**
   * 获取DataNode的HTTP服务端口。
   * 
   * @return HTTP端口字符串
   */
  public String getHttpPort();

  /**
   * 获取DataNode的数据传输端口。
   *
   * @return 数据传输端口字符串
   */
  String getDataPort();

  /**
   * 获取当前DataNode连接的所有NameNode的IP地址。
   * 
   * @return 当前DataNode正在通信的NameNode地址字符串
   */
  public String getNamenodeAddresses();

  /**
   * 获取当前DataNode的主机名。
   *
   * @return DataNode的主机名字符串
   */
  public String getDatanodeHostname();

  /**
   * 获取所有块池服务执行器的信息。
   *
   * @return 块池服务执行器信息字符串
   */
  String getBPServiceActorInfo();

  /**
   * 获取DataNode上所有存储卷的信息，返回格式请参考具体实现。
   * 
   * @return 存储卷信息字符串
   */
  public String getVolumeInfo();
  
  /**
   * 获取当前集群的ID。
   * 
   * @return 集群ID字符串
   */
  public String getClusterId();

  /**
   * 获取当前活跃的数据传输服务线程数。
   * @return 活跃数据传输线程数量
   */
  public int getXceiverCount();

  /**
   * 获取当前正在主动传输块的线程数量。
   * @return 活跃块传输线程数量
   */
  int getActiveTransferThreadCount();

  /**
   * 获取当前正在运行的数据块复制/重构任务估算数量。
   * @return 进行中的数据传输任务数
   */
  public int getXmitsInProgress();

  /**
   * 获取DataNode节点层面的网络错误统计。
   * @return 按目标节点分组的网络错误计数，第一层key为目标节点地址，第二层key为错误类型，value为错误次数
   */
  public Map<String, Map<String, Long>> getDatanodeNetworkCounts();

  /**
   * 获取磁盘均衡器的运行状态，返回格式请参考具体实现。
   *
   * @return 磁盘均衡器状态字符串
   */
  String getDiskBalancerStatus();

  /**
   * 获取DataNode作为管道倒数第二个节点时，向下游发送数据包的平均指标（如平均耗时）。
   * 返回示例为JSON格式，包含每个下游节点的滚动平均耗时。
   *
   * @return 平均指标JSON字符串
   */
  String getSendPacketDownstreamAvgInfo();

  /**
   * 获取DataNode中被判定为慢盘的磁盘列表。
   *
   * @return 慢盘列表字符串
   */
  String getSlowDisks();

  /**
   * 获取安全认证是否开启的状态。
   *
   * @return true表示安全认证已启用
   */
  boolean isSecurityEnabled();

  /**
   * 获取DataNode进程的启动时间戳。
   *
   * @return DataNode启动时间（毫秒时间戳）
   */
  long getDNStartedTimeInMillis();
}