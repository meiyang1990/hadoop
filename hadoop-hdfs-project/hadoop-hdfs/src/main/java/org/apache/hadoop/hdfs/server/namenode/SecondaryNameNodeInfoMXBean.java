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
 * SecondaryNameNode的JMX监控信息MXBean接口
 * 提供SecondaryNameNode运行状态指标的JMX暴露能力，供监控系统集成
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SecondaryNameNodeInfoMXBean extends VersionInfoMXBean {
  /**
   * 获取SecondaryNameNode的主机地址与端口，格式为"主机:端口"
   * @return 冒号分隔的主机地址和端口字符串
   */
  public String getHostAndPort();

  /**
   * 获取是否开启安全认证
   * @return true表示安全认证已启用，false表示未启用
   */
  boolean isSecurityEnabled();

  /**
   * 获取SecondaryNameNode启动时间戳
   * @return SecondaryNameNode启动时间的毫秒时间戳
   */
  public long getStartTime();

  /**
   * 获取上一次检查点完成的时间戳
   * @return 上一次检查点操作完成的毫秒时间戳
   */
  public long getLastCheckpointTime();

  /**
   * 获取距离上一次检查点经过的时间
   * @return 距离上一次检查点的毫秒数，若未进行过检查点则返回-1
   */
  public long getLastCheckpointDeltaMs();

  /**
   * 获取存储检查点镜像的目录列表
   * @return 存储检查点镜像的目录路径数组
   */
  public String[] getCheckpointDirectories();

  /**
   * 获取存储检查点编辑日志的目录列表
   * @return 存储编辑日志的目录路径数组
   */
  public String[] getCheckpointEditlogDirectories();
}