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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件级注释：HDFS节点间通信协议中，DataNode命令的抽象基类。
 * 所有NameNode下发给DataNode的操作命令都继承自此类，封装了命令的通用结构，
 * 用于NameNode向DataNode下达执行指令，指示DataNode完成指定操作。
 */
/**
 * DataNode命令抽象基类，所有NameNode下发给DataNode的命令都继承此类。
 * 由NameNode发起，通知DataNode执行对应操作，是HDFS节点间控制指令的基础抽象。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class DatanodeCommand extends ServerCommand {

  /**
   * 构造DataNode命令对象，指定命令动作类型
   * @param action 命令动作类型标识
   */
  DatanodeCommand(int action) {
    super(action);
  }
}