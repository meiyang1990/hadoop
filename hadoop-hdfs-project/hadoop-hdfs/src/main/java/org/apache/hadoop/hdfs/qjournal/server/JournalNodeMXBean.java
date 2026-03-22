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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * 文件说明：JournalNode节点的JMX管理接口，用于通过JMX暴露JournalNode运行状态信息
 * 
 * JMX管理接口，用于对外暴露JournalNode的运行时指标与状态信息
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface JournalNodeMXBean {
  
  /**
   * 获取JournalNode上所有日志节点的状态信息，包括是否已格式化等
   * 
   * @return 每个日志的状态信息字符串
   */
  String getJournalsStatus();

  /**
   * 获取JournalNode节点的监听主机与端口信息
   *
   * @return 格式为主机:端口的字符串
   */
  String getHostAndPort();

  /**
   * 获取当前JournalNode服务的所有集群ID列表，一个JournalNode可支持多个集群的日志存储
   *
   * @return 集群ID列表
   */
  List<String> getClusterIds();

  /**
   * 获取当前运行的Hadoop版本号
   *
   * @return Hadoop版本字符串
   */
  String getVersion();

  /**
   * 获取JournalNode节点的启动时间戳
   *
   * @return 启动时间，单位毫秒
   */
  long getJNStartedTimeInMillis();

  /**
   * 获取JournalNode所有日志的存储信息，包含布局版本、命名空间ID、集群ID、文件系统创建时间等元数据
   *
   * @return 与各日志关联的存储信息列表
   */
  List<String> getStorageInfos();
}