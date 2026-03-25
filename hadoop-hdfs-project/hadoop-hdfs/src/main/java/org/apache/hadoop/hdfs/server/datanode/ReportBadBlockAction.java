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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.RemoteException;

/**
 * 报告损坏块的动作，由BPOfferService发给BPServiceActor，用于向NameNode上报本节点发现的坏块
 * 实现BPServiceActorAction接口，可被BPServiceActor线程执行
 */
public class ReportBadBlockAction implements BPServiceActorAction {

  private final ExtendedBlock block;
  private final String storageUuid;
  private final StorageType storageType;

  /**
   * 构造上报坏块的动作对象
   * @param block 需要上报的坏块信息
   * @param storageUuid 坏块所在存储的UUID
   * @param storageType 坏块所在存储的类型
   */
  public ReportBadBlockAction(ExtendedBlock block, String storageUuid, 
      StorageType storageType) {
    this.block = block;
    this.storageUuid = storageUuid;
    this.storageType = storageType;
  }

  /**
   * 执行上报坏块到NameNode的动作
   * @param bpNamenode NameNode协议客户端
   * @param bpRegistration 当前DataNode注册信息
   * @throws BPServiceActorActionException 上报失败时抛出异常
   */
  @Override
  public void reportTo(DatanodeProtocolClientSideTranslatorPB bpNamenode, 
    DatanodeRegistration bpRegistration) throws BPServiceActorActionException {
    // 如果注册信息为空，直接返回不执行上报
    if (bpRegistration == null) {
      return;
    }
    // 构造只包含当前DataNode的数组，用于构建LocatedBlock
    DatanodeInfo[] dnArr = {new DatanodeInfoBuilder()
        .setNodeID(bpRegistration).build()};
    // 构造存储UUID数组
    String[] uuids = { storageUuid };
    // 构造存储类型数组
    StorageType[] types = { storageType };
    // 构造待上报的坏块LocatedBlock对象
    LocatedBlock[] locatedBlock = { new LocatedBlock(block,
        dnArr, uuids, types) };

    try {
      // 调用NameNode接口上报坏块列表
      bpNamenode.reportBadBlocks(locatedBlock);
    } catch (RemoteException re) {
      // 捕获远程异常，记录日志不抛出，避免影响后续处理
      DataNode.LOG.info("reportBadBlock encountered RemoteException for "
          + "block:  " + block , re);
    } catch (IOException e) {
      // IO异常抛出，通知上层上报失败
      throw new BPServiceActorActionException("Failed to report bad block "
          + block + " to namenode.", e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((block == null) ? 0 : block.hashCode());
    result = prime * result
        + ((storageType == null) ? 0 : storageType.hashCode());
    result = prime * result
        + ((storageUuid == null) ? 0 : storageUuid.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ReportBadBlockAction)) {
      return false;
    }
    ReportBadBlockAction other = (ReportBadBlockAction) obj;
    if (block == null) {
      if (other.block != null) {
        return false;
      }
    } else if (!block.equals(other.block)) {
      return false;
    }
    if (storageType != other.storageType) {
      return false;
    }
    if (storageUuid == null) {
      if (other.storageUuid != null) {
        return false;
      }
    } else if (!storageUuid.equals(other.storageUuid)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("block", block)
        .append("storageUuid", storageUuid)
        .append("storageType", storageType)
        .toString();
  }
}