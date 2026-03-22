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
 * @file org.apache.hadoop.hdfs.server.protocol
 * HDFS服务端间通信命令抽象基类，所有NameNode发给其他服务节点的命令都继承此类
 * 所有命令的具体动作由对应通信协议定义，封装了统一的命令结构基础
 * 
 * @see DatanodeProtocol
 * @see NamenodeProtocol
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ServerCommand {
  /** 命令动作编码，具体含义由协议定义 */
  private final int action;

  /**
   * 构造服务端命令对象，动作编码由具体协议定义
   * @see DatanodeProtocol
   * @see NamenodeProtocol
   * @param action 协议定义的动作编码
   */
  public ServerCommand(int action) {
    this.action = action;
  }

  /**
   * 获取该命令对应的动作编码
   * @return 动作编码，具体含义由对应协议定义
   */
  public int getAction() {
    return this.action;
  }

  /**
   * 将命令转换为字符串形式，用于日志输出
   * @return 命令的字符串描述，包含命令类型和动作编码
   */
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName())
        .append("/")
        .append(action);
    return sb.toString();
  }
}