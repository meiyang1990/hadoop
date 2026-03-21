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

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;

/**
 * NodeManager节点资源监控接口，定义获取节点当前资源使用情况的抽象能力。
 * 各实现类可通过不同监控方式采集节点实际资源利用率，支撑YARN节点健康检查和资源调度。
 */
public interface NodeResourceMonitor extends Service {
  /**
   * 获取当前节点整体资源利用率信息，包含CPU、内存等资源的使用情况。
   * @return 当前节点的资源利用率信息
   */
  public ResourceUtilization getUtilization();
}