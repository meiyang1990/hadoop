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
 * HDFS注册命令类，封装NameNode发给DataNode的注册指令。
 * 该命令用于指示DataNode向NameNode重新发起注册流程。
 * 本命令在同一个心跳响应中不能与其他命令组合使用，因为DataNode处理完该命令后会跳过同响应中其他命令。
 * 属于HDFS内部节点间通信协议，用于DataNode与NameNode之间的指令交互。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegisterCommand extends DatanodeCommand {
  /**
   * 单例注册命令实例，供NameNode直接复用返回给DataNode
   */
  public static final DatanodeCommand REGISTER = new RegisterCommand();

  /**
   * 构造注册命令对象，设置命令类型为DNA_REGISTER
   */
  public RegisterCommand() {
    super(DatanodeProtocol.DNA_REGISTER);
  }
}