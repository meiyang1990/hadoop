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

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;

/**
 * HDFS未注册节点异常类，当未完成注册的节点尝试访问NameNode时抛出该异常
 * 包括JournalNode、DataNode等各类集群节点未注册访问的场景
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class UnregisteredNodeException extends IOException {
  private static final long serialVersionUID = -5620209396945970810L;

  /**
   * 构造函数，基于Journal节点信息创建未注册异常
   * @param info 未注册的Journal节点信息
   */
  public UnregisteredNodeException(JournalInfo info) {
    super("Unregistered server: " + info.toString());
  }
  
  /**
   * 构造函数，基于节点注册信息创建未注册异常
   * @param nodeReg 未注册节点的注册信息
   */
  public UnregisteredNodeException(NodeRegistration nodeReg) {
    super("Unregistered server: " + nodeReg.toString());
  }

  /**
   * 构造函数，用于存储ID冲突场景：当新DataNode声称使用了已存在节点的存储ID时抛出
   *  
   * @param nodeID 未注册的DataNode标识
   * @param storedNode 系统中已存在该存储ID对应DataNode信息
   */
  public UnregisteredNodeException(DatanodeID nodeID, DatanodeInfo storedNode) {
    super("Data node " + nodeID + " is attempting to report storage ID " 
          + nodeID.getDatanodeUuid() + ". Node "
          + storedNode + " is expected to serve this storage.");
  }
}