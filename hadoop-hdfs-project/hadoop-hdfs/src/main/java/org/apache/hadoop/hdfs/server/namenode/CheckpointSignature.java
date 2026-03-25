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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;

import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

/**
 * 文件级注释：HDFS NameNode检查点签名类，用于唯一标识检查点事务，辅助SecondaryNameNode与主NameNode之间的一致性校验
 *
 * A unique signature intended to identify checkpoint transactions.
 */
@InterfaceAudience.Private
/**
 * 检查点签名类，封装检查点元数据信息，用于在检查点过程中校验一致性，实现Comparable接口支持比较
 */
@InterfaceAudience.Private
public class CheckpointSignature extends StorageInfo
    implements Comparable<CheckpointSignature> { 

  private static final String FIELD_SEPARATOR = ":";
  private static final int NUM_FIELDS = 7;
  String blockpoolID = "";
  long mostRecentCheckpointTxId;
  long curSegmentTxId;

  /**
   * 从FSImage构造检查点签名，提取FSImage中存储的元数据信息
   * @param fsImage 要提取信息的FSImage对象
   */
  CheckpointSignature(FSImage fsImage) {
    super(fsImage.getStorage());
    blockpoolID = fsImage.getBlockPoolID();
    
    mostRecentCheckpointTxId = fsImage.getStorage().getMostRecentCheckpointTxId();
    curSegmentTxId = fsImage.getEditLog().getCurSegmentTxId();
  }

  /**
   * 从字符串解析构造检查点签名，解析序列化后的字符串反序列化出各个字段
   * @param str 序列化后的签名字符串
   */
  CheckpointSignature(String str) {
    super(NodeType.NAME_NODE);
    // 按分隔符切分各个字段
    String[] fields = str.split(FIELD_SEPARATOR);
    assert fields.length == NUM_FIELDS :
      "Must be " + NUM_FIELDS + " fields in CheckpointSignature";
    int i = 0;
    // 解析布局版本号
    layoutVersion = Integer.parseInt(fields[i++]);
    // 解析命名空间ID
    namespaceID = Integer.parseInt(fields[i++]);
    // 解析创建时间
    cTime = Long.parseLong(fields[i++]);
    // 解析最近一次检查点事务ID
    mostRecentCheckpointTxId  = Long.parseLong(fields[i++]);
    // 解析当前日志段起始事务ID
    curSegmentTxId  = Long.parseLong(fields[i++]);
    // 解析集群ID
    clusterID = fields[i++];
    // 解析块池ID
    blockpoolID = fields[i];
  }

  /**
   * 全参数构造检查点签名，使用已有信息构造签名对象
   * @param info 存储信息基类
   * @param blockpoolID 块池ID
   * @param mostRecentCheckpointTxId 最近一次检查点事务ID
   * @param curSegmentTxId 当前编辑日志段起始事务ID
   */
  public CheckpointSignature(StorageInfo info, String blockpoolID,
      long mostRecentCheckpointTxId, long curSegmentTxId) {
    super(info);
    this.blockpoolID = blockpoolID;
    this.mostRecentCheckpointTxId = mostRecentCheckpointTxId;
    this.curSegmentTxId = curSegmentTxId;
  }

  /**
   * Get the cluster id from CheckpointSignature
   * @return the cluster id
   */
  @Override
  public String getClusterID() {
    return clusterID;
  }

  /**
   * Get the block pool id from CheckpointSignature
   * @return the block pool id
   */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  /**
   * 获取最近一次完成的检查点事务ID
   * @return 最近检查点事务ID
   */
  public long getMostRecentCheckpointTxId() {
    return mostRecentCheckpointTxId;
  }

  /**
   * 获取当前编辑日志段的起始事务ID
   * @return 当前日志段起始事务ID
   */
  public long getCurSegmentTxId() {
    return curSegmentTxId;
  }

  /**
   * Set the block pool id of CheckpointSignature.
   * 
   * @param blockpoolID the new blockpool id
   */
  public void setBlockpoolID(String blockpoolID) {
    this.blockpoolID = blockpoolID;
  }
  
  @Override
  public String toString() {
    // 将所有字段拼接为序列化字符串，使用分隔符分隔
    return String.valueOf(layoutVersion) + FIELD_SEPARATOR
         + String.valueOf(namespaceID) + FIELD_SEPARATOR
         + String.valueOf(cTime) + FIELD_SEPARATOR
         + String.valueOf(mostRecentCheckpointTxId) + FIELD_SEPARATOR
         + String.valueOf(curSegmentTxId) + FIELD_SEPARATOR
         + clusterID + FIELD_SEPARATOR
         + blockpoolID ;
  }

  /**
   * 校验存储版本和创建时间是否匹配
   * @param si 待比对的存储信息
   * @return 匹配返回true，否则返回false
   * @throws IO 异常当校验不匹配时抛出异常
   */
  boolean storageVersionMatches(StorageInfo si) throws IOException {
    return (layoutVersion == si.layoutVersion) && (cTime == si.cTime);
  }

  /**
   * 校验是否属于同一个集群，匹配命名空间ID、集群ID、块池ID都一致
   * @param si 待比对的FSImage
   * @return 属于同一集群返回true，否则返回false
   */
  boolean isSameCluster(FSImage si) {
    return namespaceID == si.getStorage().namespaceID &&
      clusterID.equals(si.getClusterID()) &&
      blockpoolID.equals(si.getBlockPoolID());
  }

  /**
   * 校验命名空间ID是否匹配
   * @param si 待比对的FSImage
   * @return 匹配返回true，否则返回false
   */
  boolean namespaceIdMatches(FSImage si) {
    return namespaceID == si.getStorage().namespaceID;
  }

  /**
   * 整体校验检查点存储信息一致性，不匹配则抛出IO异常
   * @param si 待校验的FSImage
   * @throws IOException 一致性校验失败抛出异常
   */
  void validateStorageInfo(FSImage si) throws IOException {
    if (!isSameCluster(si)
        || !storageVersionMatches(si.getStorage())) {
      throw new IOException("Inconsistent checkpoint fields.\n"
          + "LV = " + layoutVersion + " namespaceID = " + namespaceID
          + " cTime = " + cTime
          + " ; clusterId = " + clusterID
          + " ; blockpoolId = " + blockpoolID
          + ".\nExpecting respectively: "
          + si.getStorage().layoutVersion + "; " 
          + si.getStorage().namespaceID + "; " + si.getStorage().cTime
          + "; " + si.getClusterID() + "; " 
          + si.getBlockPoolID() + ".");
    }
  }

  //
  // Comparable interface
  //
  /**
   * 按所有字段依次比较两个检查点签名，实现比较逻辑
   * @param o 待比较的另一个检查点签名
   * @return 比较结果，0表示相等
   */
  @Override
  public int compareTo(CheckpointSignature o) {
    // 使用ComparisonChain链式依次比较所有字段
    return ComparisonChain.start()
      .compare(layoutVersion, o.layoutVersion)
      .compare(namespaceID, o.namespaceID)
      .compare(cTime, o.cTime)
      .compare(mostRecentCheckpointTxId, o.mostRecentCheckpointTxId)
      .compare(curSegmentTxId, o.curSegmentTxId)
      .compare(clusterID, o.clusterID)
      .compare(blockpoolID, o.blockpoolID)
      .result();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CheckpointSignature)) {
      return false;
    }
    // 复用compareTo结果判断相等
    return compareTo((CheckpointSignature)o) == 0;
  }

  @Override
  public int hashCode() {
    // 异或所有字段生成哈希值
    return layoutVersion ^ namespaceID ^
            (int)(cTime ^ mostRecentCheckpointTxId ^ curSegmentTxId)
            ^ clusterID.hashCode() ^ blockpoolID.hashCode();
  }
}