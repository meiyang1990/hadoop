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
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;

/**
 * HDFS检查点命令类，封装NameNode对SecondaryNameNode/备份节点检查点请求的响应信息
 * <p>
 * 当SecondaryNameNode/备份节点向NameNode发起{@link NamenodeProtocol#startCheckpoint(NamenodeRegistration)}
 * 检查点启动请求后，NameNode返回该命令作为响应，包含检查点执行所需的全部参数：
 * <ul>
 * <li>{@link CheckpointSignature} 检查点签名，用于标识当前检查点的版本信息</li>
 * <li>标志位，指示检查点开始前是否需要清理旧的备份镜像</li>
 * <li>标志位，指示检查点完成后是否需要将生成的新镜像传回NameNode</li>
 * </ul>
 * 该类是HDFS检查点机制中NameNode和SecondaryNameNode之间通信的核心数据结构
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CheckpointCommand extends NamenodeCommand {
  private final CheckpointSignature cSig;
  private final boolean needToReturnImage;

  /**
   * 无参构造器，构造空检查点命令对象
   */
  public CheckpointCommand() {
    this(null, false);
  }

  /**
   * 构造完整检查点命令对象
   * @param sig 检查点签名，标识当前检查点的元数据信息
   * @param needToReturnImg 指示检查点完成后是否需要将新镜像传回NameNode
   */
  public CheckpointCommand(CheckpointSignature sig,
                           boolean needToReturnImg) {
    super(NamenodeProtocol.ACT_CHECKPOINT);
    this.cSig = sig;
    this.needToReturnImage = needToReturnImg;
  }

  /**
   * 获取检查点签名，用于确保双方对本次检查点达成一致，避免版本不匹配
   * @return 当前检查点的签名对象
   */
  public CheckpointSignature getSignature() {
    return cSig;
  }

  /**
   * 检查是否需要在检查点完成后将新镜像传回NameNode
   * @return true表示需要将检查点镜像传回NameNode，false不需要
   */
  public boolean needToReturnImage() {
    return needToReturnImage;
  }
}