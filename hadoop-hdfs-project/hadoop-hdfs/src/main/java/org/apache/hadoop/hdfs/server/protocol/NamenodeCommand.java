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
 * HDFS集群中NameNode间命令的抽象基类
 * 用于主NameNode向集群中其他NameNode下发操作指令，通知目标节点执行指定动作
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamenodeCommand extends ServerCommand {
  /**
   * 构造指定动作类型的NameNode命令
   * @param action 动作类型标识
   */
  public NamenodeCommand(int action) {
    super(action);
  }
}