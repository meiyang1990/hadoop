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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * NodeManager JMX监控管理接口，对外暴露NodeManager运行状态指标，供JMX生态采集监控。
 * 用户无需自行实现该接口，可直接通过JMX API获取对应监控信息。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface NodeManagerMXBean {
  /**
   * 获取当前NodeManager是否启用安全认证。
   *
   * @return true 表示安全认证已启用，false表示未启用
   * */
  boolean isSecurityEnabled();
}