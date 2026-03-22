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
 * 文件所属模块：HDFS服务端协议
 * 核心职责：定义名称节点下发给数据节点的删除SPS待处理任务命令，用于停止正在进行的块存储移动任务
 *
 * 该类表示NameNode通知DataNode清理SPSWorker中待处理的块存储移动队列的命令，
 * SPS是HDFS中存储策略满足器(Storage Policy Satisfier)，负责执行块的存储层迁移任务。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DropSPSWorkCommand extends DatanodeCommand {
  /**
   * 全局单例命令实例，NameNode下发命令时复用该实例
   */
  public static final DropSPSWorkCommand DNA_DROP_SPS_WORK_COMMAND =
      new DropSPSWorkCommand();

  /**
   * 构造删除SPS待处理任务命令，注册对应命令类型
   */
  public DropSPSWorkCommand() {
    super(DatanodeProtocol.DNA_DROP_SPS_WORK_COMMAND);
  }
}