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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.Service;

/**
 * YARN ResourceManager 嵌入式领导者选举接口，所有内置选举实现都需要实现该接口。
 * 用于HA场景下多ResourceManager的主节点选举。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface EmbeddedElector extends Service{
  /**
   * 退出并重新加入领导者选举流程。
   */
  void rejoinElection();

  /**
   * 获取选举器与Zookeeper的连接状态信息。
   *
   * @return zookeeper连接状态字符串
   */
  String getZookeeperConnectionState();
}