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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;

import org.apache.hadoop.classification.VisibleForTesting;

/** 
 * DatanodeRegistration 包含NameNode识别和验证DataNode所需的全部信息
 * DataNode每次向NameNode发起通信请求时都会携带该信息
 * 本类是HDFS DataNode向NameNode注册时的信息承载对象
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeRegistration extends DatanodeID
    implements NodeRegistration {

  /** DataNode存储信息，包含存储版本等元数据 */
  private final StorageInfo storageInfo;
  /** 导出的块密钥，用于数据块访问认证 */
  private ExportedBlockKeys exportedKeys;
  /** DataNode当前运行的软件版本 */
  private final String softwareVersion;
  /** 关联的命名空间信息 */
  private NamespaceInfo nsInfo;

  /**
   * 构造方法，仅用于单元测试，基于已有注册信息生成新的DataNode注册对象
   * @param uuid 新的DataNode UUID
   * @param dnr 原有的DataNode注册信息
   */
  @VisibleForTesting
  public DatanodeRegistration(String uuid, DatanodeRegistration dnr) {
    this(new DatanodeID(uuid, dnr),
         dnr.getStorageInfo(),
         dnr.getExportedKeys(),
         dnr.getSoftwareVersion());
  }

  /**
   * 构造完整的DataNode注册信息对象
   * @param dn DataNodeID，包含DataNode的基础标识信息
   * @param info 存储信息
   * @param keys 导出的块密钥
   * @param softwareVersion DataNode软件版本
   */
  public DatanodeRegistration(DatanodeID dn, StorageInfo info,
      ExportedBlockKeys keys, String softwareVersion) {
    super(dn);
    this.storageInfo = info;
    this.exportedKeys = keys;
    this.softwareVersion = softwareVersion;
  }

  /**
   * 获取DataNode的存储信息
   * @return DataNode存储信息对象
   */
  public StorageInfo getStorageInfo() {
    return storageInfo;
  }

  /**
   * 更新导出的块密钥
   * @param keys 新的块密钥
   */
  public void setExportedKeys(ExportedBlockKeys keys) {
    this.exportedKeys = keys;
  }

  /**
   * 获取导出的块密钥
   * @return 当前块密钥
   */
  public ExportedBlockKeys getExportedKeys() {
    return exportedKeys;
  }
  
  /**
   * 获取DataNode的软件版本
   * @return DataNode软件版本字符串
   */
  public String getSoftwareVersion() {
    return softwareVersion;
  }

  @Override // NodeRegistration
  /**
   * 获取存储布局版本，实现NodeRegistration接口
   * @return HDFS存储布局版本号
   */
  public int getVersion() {
    return storageInfo.getLayoutVersion();
  }

  /**
   * 设置关联的命名空间信息
   * @param nsInfo 命名空间信息对象
   */
  public void setNamespaceInfo(NamespaceInfo nsInfo) {
    this.nsInfo = nsInfo;
  }

  /**
   * 获取关联的命名空间信息
   * @return 命名空间信息对象
   */
  public NamespaceInfo getNamespaceInfo() {
    return nsInfo;
  }
  
  @Override // NodeRegistration
  /**
   * 获取注册ID，用于标识DataNode存储版本，实现NodeRegistration接口
   * @return 注册ID字符串
   */
  public String getRegistrationID() {
    return Storage.getRegistrationID(storageInfo);
  }

  @Override // NodeRegistration
  /**
   * 获取DataNode数据传输地址，实现NodeRegistration接口
   * @return DataNode数据传输地址字符串
   */
  public String getAddress() {
    return getXferAddr();
  }

  @Override
  /**
   * 序列化为可读字符串，用于日志输出调试
   * @return 包含所有注册信息的字符串
   */
  public String toString() {
    return getClass().getSimpleName()
      + "(" + super.toString()
      + ", datanodeUuid=" + getDatanodeUuid()
      + ", infoPort=" + getInfoPort()
      + ", infoSecurePort=" + getInfoSecurePort()
      + ", ipcPort=" + getIpcPort()
      + ", storageInfo=" + storageInfo
      + ")";
  }

  @Override
  /**
   * 相等性判断，继承父类DatanodeID的相等逻辑
   * @param to 待比较对象
   * @return 是否相等
   */
  public boolean equals(Object to) {
    return super.equals(to);
  }
  @Override
  /**
   * 生成哈希码，继承父类DatanodeID的哈希逻辑
   * @return 哈希码
   */
  public int hashCode() {
    return super.hashCode();
  }
}