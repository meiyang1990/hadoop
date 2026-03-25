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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.namenode.INodeFile.HeaderFormat;

/**
 * @file INodeFileAttributes.java
 * @brief HDFS文件INode属性接口，定义文件元数据的访问接口
 *
 * 该接口继承自INodeAttributes，扩展了文件特有的属性访问方法，
 * 用于描述HDFS中文件类型INode的所有核心元数据属性，包括副本数、块布局、纠删码策略等。
 */
@InterfaceAudience.Private
public interface INodeFileAttributes extends INodeAttributes {
  /**
   * 获取文件的副本系数
   * @return 文件副本数
   */
  short getFileReplication();

  /**
   * 判断文件是否采用纠删码条纹存储
   * @return true表示条纹存储(纠删码)，false表示连续块存储(多副本)
   */
  boolean isStriped();

  /**
   * 获取文件的块类型
   * @return 块类型（连续块、条纹块等）
   */
  BlockType getBlockType();

  /**
   * 获取纠删码策略ID
   * @return 纠删码策略标识
   */
  byte getErasureCodingPolicyID();

  /**
   * 获取文件的首选块大小（单位：字节）
   * @return 首选块大小（字节）
   */
  long getPreferredBlockSize();

  /**
   * 获取编码为long类型的头信息
   * @return 打包后的头信息long值
   */
  long getHeaderLong();

  /**
   * 比较当前实例与另一个文件属性实例的元数据是否相等
   * @param other 另一个文件属性实例
   * @return 元数据相等返回true，否则返回false
   */
  boolean metadataEquals(INodeFileAttributes other);

  /**
   * 获取本地存储策略ID
   * @return 存储策略标识
   */
  byte getLocalStoragePolicyID();

  /**
   * @class SnapshotCopy
   * @brief 文件INode属性的快照副本，用于保存文件节点做快照前的属性状态
   *
   * 当文件节点被修改并生成快照时，该类会存储修改前的完整属性副本，
   * 支持快照功能读取历史版本的文件元数据。继承自INodeAttributes的快照副本，
   * 实现了INodeFileAttributes接口，提供文件属性的完整访问能力。
   */
  static class SnapshotCopy extends INodeAttributes.SnapshotCopy
      implements INodeFileAttributes {
    // 打包存储所有文件属性的头信息long值
    private final long header;

    /**
     * 构造函数，从独立属性构造快照副本
     * @param name 文件名
     * @param permissions 权限状态
     * @param aclFeature ACL特性
     * @param modificationTime 修改时间
     * @param accessTime 访问时间
     * @param replication 副本系数
     * @param ecPolicyID 纠删码策略ID
     * @param preferredBlockSize 首选块大小
     * @param storagePolicyID 存储策略ID
     * @param xAttrsFeature 扩展属性特性
     * @param blockType 块类型
     */
    public SnapshotCopy(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, long accessTime,
        Short replication, Byte ecPolicyID, long preferredBlockSize,
        byte storagePolicyID, XAttrFeature xAttrsFeature, BlockType blockType) {
      super(name, permissions, aclFeature, modificationTime, accessTime, 
          xAttrsFeature);
      // 根据块类型、副本数、纠删码ID计算布局冗余信息
      final long layoutRedundancy = HeaderFormat.getBlockLayoutRedundancy(
          blockType, replication, ecPolicyID);
      // 将所有属性打包为long类型的头信息
      header = HeaderFormat.toLong(preferredBlockSize, layoutRedundancy,
          storagePolicyID);
    }

    /**
     * 构造函数，从现有INodeFile对象生成快照副本
     * @param file 源文件INode对象
     */
    public SnapshotCopy(INodeFile file) {
      super(file);
      // 直接复制源文件的头信息
      this.header = file.getHeaderLong();
    }

    @Override
    public boolean isDirectory() {
      return false;
    }

    @Override
    public short getFileReplication() {
      return HeaderFormat.getReplication(header);
    }

    @Override
    public boolean isStriped() {
      return HeaderFormat.isStriped(header);
    }

    @Override
    public BlockType getBlockType() {
      return HeaderFormat.getBlockType(header);
    }

    @Override
    public byte getErasureCodingPolicyID() {
      if (isStriped()) {
        return HeaderFormat.getECPolicyID(header);
      }
      return -1;
    }

    @Override
    public long getPreferredBlockSize() {
      return HeaderFormat.getPreferredBlockSize(header);
    }

    @Override
    public byte getLocalStoragePolicyID() {
      return HeaderFormat.getStoragePolicyID(header);
    }

    @Override
    public long getHeaderLong() {
      return header;
    }

    @Override
    public boolean metadataEquals(INodeFileAttributes other) {
      return other != null
          && getHeaderLong()== other.getHeaderLong()
          && getPermissionLong() == other.getPermissionLong()
          && getAclFeature() == other.getAclFeature()
          && getXAttrFeature() == other.getXAttrFeature();
    }
  }
}