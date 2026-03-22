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

package org.apache.hadoop.mapreduce.v2;

/**
 * MapReduce任务日志查询参数容器，存储容器ID、应用ID、节点ID和应用所有者等日志定位所需信息
 * 用于在WebUI或日志服务中快速定位指定容器的日志文件
 */
public class LogParams {

  private String containerId;
  private String applicationId;
  private String nodeId;
  private String owner;

  /**
   * 构造日志参数对象，初始化所有日志定位参数
   * @param containerIdStr 容器ID字符串，对应运行任务的YARN容器标识
   * @param applicationIdStr 应用ID字符串，对应MapReduce作业所属YARN应用标识
   * @param nodeIdStr 节点ID字符串，对应运行容器的NodeManager节点标识
   * @param owner 应用所有者用户名，对应提交应用的用户
   */
  public LogParams(String containerIdStr, String applicationIdStr,
      String nodeIdStr, String owner) {
    this.containerId = containerIdStr;
    this.applicationId = applicationIdStr;
    this.nodeId = nodeIdStr;
    this.owner = owner;
  }

  /**
   * 获取容器ID
   * @return 容器ID字符串
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * 设置容器ID
   * @param containerId 容器ID字符串
   */
  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  /**
   * 获取应用ID
   * @return 应用ID字符串
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * 设置应用ID
   * @param applicationId 应用ID字符串
   */
  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  /**
   * 获取节点ID
   * @return 节点ID字符串
   */
  public String getNodeId() {
    return nodeId;
  }

  /**
   * 设置节点ID
   * @param nodeId 节点ID字符串
   */
  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  /**
   * 获取应用所有者用户名
   * @return 应用所有者用户名
   */
  public String getOwner() {
    return this.owner;
  }

  /**
   * 设置应用所有者用户名
   * @param owner 应用所有者用户名
   * @return 设置后的应用所有者用户名
   */
  public String setOwner(String owner) {
    return this.owner = owner;
  }
}