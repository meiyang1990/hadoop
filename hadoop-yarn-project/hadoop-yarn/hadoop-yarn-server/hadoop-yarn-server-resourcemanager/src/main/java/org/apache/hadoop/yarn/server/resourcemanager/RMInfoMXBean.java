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
package org.apache.hadoop.yarn.server.resourcemanager;

/**
 * ResourceManager运行状态信息的JMX MXBean接口，
 * 提供RM核心信息的监控指标暴露，供JMX监控系统采集。
 */
public interface RMInfoMXBean {

  /**
   * 获取ResourceManager当前运行状态。
   * @return ResourceManager当前状态字符串
   */
  String getState();

  /**
   * 获取ResourceManager服务地址，格式为主机名:端口。
   * @return 冒号分隔的主机名和端口
   */
  String getHostAndPort();

  /**
   * 获取安全认证是否启用。
   * @return true表示安全认证已启用，false表示未启用
   */
  boolean isSecurityEnabled();
}