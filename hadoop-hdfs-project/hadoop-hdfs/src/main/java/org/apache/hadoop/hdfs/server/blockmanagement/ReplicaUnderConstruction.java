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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * 文件级注释：HDFS构建中副本信息存储类，用于记录处于构建状态的块副本相关信息。
 * 当数据块正在写入流水线中，或者处于恢复过程中时，使用此类跟踪副本的状态和预期位置。
 *
 * 类级注释：记录正在构建中的块副本信息，保存了副本预期存储位置、状态以及恢复相关标记。
 * 继承自Block类，扩展了构建过程中需要的状态和位置信息，用于NameNode管理写入过程中的块副本。
 */
class ReplicaUnderConstruction extends Block {
  /** 副本预期存储的DataNode存储位置信息 */
  private final DatanodeStorageInfo expectedLocation;
  /** 当前副本的状态，由DataNode上报 */
  private HdfsServerConstants.ReplicaState state;
  /** 是否被选择为块恢复过程中的主副本 */
  private boolean chosenAsPrimary;

  /**
   * 构造方法，初始化正在构建中的副本对象
   * @param block 原始块信息
   * @param target 预期存储该副本的DataNode存储位置
   * @param state 副本初始状态
   */
  ReplicaUnderConstruction(Block block,
      DatanodeStorageInfo target,
      HdfsServerConstants.ReplicaState state) {
    super(block);
    this.expectedLocation = target;
    this.state = state;
    this.chosenAsPrimary = false;
  }

  /**
   * 获取块分配时指定的预期存储位置，该位置决定了写入流水线的顺序
   * @return 预期的DataNode存储位置信息
   */
  DatanodeStorageInfo getExpectedStorageLocation() {
    return expectedLocation;
  }

  /**
   * 获取DataNode上报的当前副本状态
   * @return 副本当前状态
   */
  HdfsServerConstants.ReplicaState getState() {
    return state;
  }

  /**
   * 获取当前副本是否被选为块恢复的主副本
   * @return true表示是主副本，false表示不是
   */
  boolean getChosenAsPrimary() {
    return chosenAsPrimary;
  }

  /**
   * 设置当前副本的状态
   * @param s 新的副本状态
   */
  void setState(HdfsServerConstants.ReplicaState s) {
    state = s;
  }

  /**
   * 设置当前副本是否为块恢复的主副本
   * @param chosenAsPrimary 是否为主副本标记
   */
  void setChosenAsPrimary(boolean chosenAsPrimary) {
    this.chosenAsPrimary = chosenAsPrimary;
  }

  /**
   * 检查当前副本所属的DataNode是否存活
   * @return true表示DataNode存活，false表示已宕机
   */
  boolean isAlive() {
    return expectedLocation.getDatanodeDescriptor().isAlive();
  }

  @Override // Block
  public int hashCode() {
    return super.hashCode();
  }

  @Override // Block
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(50);
    appendStringTo(b);
    return b.toString();
  }

  @Override
  public void appendStringTo(StringBuilder sb) {
    sb.append("ReplicaUC[")
        .append(expectedLocation)
        .append("|")
        .append(state)
        .append("]");
  }
}