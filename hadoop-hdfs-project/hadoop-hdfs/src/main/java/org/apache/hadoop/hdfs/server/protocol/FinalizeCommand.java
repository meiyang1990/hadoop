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
 * 文件级注释：HDFS节点间通信中Finalize命令的封装，用于NameNode向DataNode通知完成块池的升级最终化操作
 *
 * Finalize命令封装类，NameNode通过该命令指示DataNode完成指定块池的升级最终化处理
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FinalizeCommand extends DatanodeCommand {
  /** 目标块池ID */
  String blockPoolId;
  /**
   * 默认构造函数，构造命令类型为DNA_FINALIZE的Finalize命令
   */
  private FinalizeCommand() {
    super(DatanodeProtocol.DNA_FINALIZE);
  }
  
  /**
   * 带块池ID的构造函数，构造指定块池的Finalize命令
   * @param bpid 目标块池ID
   */
  public FinalizeCommand(String bpid) {
    super(DatanodeProtocol.DNA_FINALIZE);
    blockPoolId = bpid;
  }
  
  /**
   * 获取需要完成最终化的块池ID
   * @return 目标块池ID
   */
  public String getBlockPoolId() {
    return blockPoolId;
  }
}