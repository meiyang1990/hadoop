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

package org.apache.hadoop.mapreduce.v2.api.records;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * MapReduce ApplicationMaster 信息接口，存储AM的基本运行信息。
 * 核心职责是记录ApplicationMaster的启动信息、运行位置和标识，供ResourceManager客户端查询使用。
 */
public interface AMInfo {
  /**
   * 获取ApplicationMaster所属应用尝试的ID
   * @return 应用尝试ID
   */
  public ApplicationAttemptId getAppAttemptId();
  /**
   * 获取ApplicationMaster的启动时间戳
   * @return 启动时间（毫秒时间戳）
   */
  public long getStartTime();
  /**
   * 获取ApplicationMaster运行所在容器的ID
   * @return 容器ID
   */
  public ContainerId getContainerId();
  /**
   * 获取ApplicationMaster所在节点的NodeManager主机地址
   * @return NodeManager主机名/IP
   */
  public String getNodeManagerHost();
  /**
   * 获取ApplicationMaster所在节点的NodeManager RPC端口
   * @return RPC端口号
   */
  public int getNodeManagerPort();
  /**
   * 获取ApplicationMaster所在节点的NodeManager HTTP端口
   * @return HTTP端口号
   */
  public int getNodeManagerHttpPort();

  /**
   * 设置ApplicationMaster所属应用尝试的ID
   * @param appAttemptId 应用尝试ID
   */
  public void setAppAttemptId(ApplicationAttemptId appAttemptId);
  /**
   * 设置ApplicationMaster的启动时间戳
   * @param startTime 启动时间（毫秒时间戳）
   */
  public void setStartTime(long startTime);
  /**
   * 设置ApplicationMaster运行所在容器的ID
   * @param containerId 容器ID
   */
  public void setContainerId(ContainerId containerId);
  /**
   * 设置ApplicationMaster所在节点的NodeManager主机地址
   * @param nmHost NodeManager主机名/IP
   */
  public void setNodeManagerHost(String nmHost);
  /**
   * 设置ApplicationMaster所在节点的NodeManager RPC端口
   * @param nmPort RPC端口号
   */
  public void setNodeManagerPort(int nmPort);
  /**
   * 设置ApplicationMaster所在节点的NodeManager HTTP端口
   * @param mnHttpPort HTTP端口号
   */
  public void setNodeManagerHttpPort(int mnHttpPort);
}