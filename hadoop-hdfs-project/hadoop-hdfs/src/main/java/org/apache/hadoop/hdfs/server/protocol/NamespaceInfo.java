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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.util.VersionInfo;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件级注释：
 * 命名空间信息类，用于NameNode响应DataNode握手请求，向DataNode返回集群命名空间元信息
 * 包含版本信息、块池ID、服务能力标记和HA状态等关键信息，是DataNode注册阶段获取集群信息的核心载体
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
/**
 * 命名空间信息类，继承StorageInfo，封装NameNode端命名空间的核心元数据信息
 * 用于DataNode与NameNode握手时，NameNode向DataNode传递集群版本、块池标识、能力支持等信息
 */
public class NamespaceInfo extends StorageInfo {
  final String  buildVersion;
  String blockPoolID = "";    // 块池ID，标识当前NameNode所属的块池
  String softwareVersion;
  long capabilities; // 支持的能力位图，按位标记NameNode支持的功能
  HAServiceState state; // NameNode当前的HA服务状态

  // 仅服务端生效，记录当前服务支持的所有能力，编译时动态生成能力位图
  private static final long CAPABILITIES_SUPPORTED = getSupportedCapabilities();

  /**
   * 收集所有支持的能力，生成能力位图
   * @return 所有支持能力按位或后的位图结果
   */
  private static long getSupportedCapabilities() {
    long mask = 0;
    // 遍历所有能力枚举，将支持的能力加入位图
    for (Capability c : Capability.values()) {
      if (c.supported) {
        mask |= c.mask;
      }
    }
    return mask;
  }

  /**
   * NameNode支持的能力枚举，每个能力对应位图中的一位，用于向下兼容
   * 标识NameNode支持的扩展功能，DataNode根据能力位图判断是否可用对应功能
   */
  public enum Capability {
    UNKNOWN(false),
    STORAGE_BLOCK_REPORT_BUFFERS(true); // 是否使用优化的ByteString缓冲区处理块报告
    private final boolean supported;
    private final long mask;
    Capability(boolean isSupported) {
      supported = isSupported;
      int bits = ordinal() - 1;
      mask = (bits < 0) ? 0 : (1L << bits);
    }
    public long getMask() {
      return mask;
    }
  }

  /**
   * 空构造函数，用于服务端构造NamespaceInfo，默认开启所有支持的能力
   */
  // defaults to enabled capabilites since this ctor is for server
  public NamespaceInfo() {
    super(NodeType.NAME_NODE);
    buildVersion = null;
    capabilities = CAPABILITIES_SUPPORTED;
  }

  /**
   * 构造函数，用于服务端构造NamespaceInfo，默认开启所有支持的能力
   * @param nsID 命名空间ID
   * @param clusterID 集群ID
   * @param bpID 块池ID
   * @param cT 命名空间创建时间戳
   * @param buildVersion 构建版本号
   * @param softwareVersion 软件版本号
   */
  // defaults to enabled capabilites since this ctor is for server
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion) {
    this(nsID, clusterID, bpID, cT, buildVersion, softwareVersion,
        CAPABILITIES_SUPPORTED);
  }

  /**
   * 构造函数，指定能力位图和HA状态
   * @param nsID 命名空间ID
   * @param clusterID 集群ID
   * @param bpID 块池ID
   * @param cT 命名空间创建时间戳
   * @param buildVersion 构建版本号
   * @param softwareVersion 软件版本号
   * @param capabilities 能力位图
   * @param st 当前HA服务状态
   */
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion,
      long capabilities, HAServiceState st) {
    this(nsID, clusterID, bpID, cT, buildVersion, softwareVersion,
        capabilities);
    this.state = st;
  }

  /**
   * 构造函数，指定完整参数和能力位图，可用于服务端和客户端
   * @param nsID 命名空间ID
   * @param clusterID 集群ID
   * @param bpID 块池ID
   * @param cT 命名空间创建时间戳
   * @param buildVersion 构建版本号
   * @param softwareVersion 软件版本号
   * @param capabilities 能力位图
   */
  // for use by server and/or client
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion,
      long capabilities) {
    super(HdfsServerConstants.NAMENODE_LAYOUT_VERSION, nsID, clusterID, cT,
        NodeType.NAME_NODE);
    blockPoolID = bpID;
    this.buildVersion = buildVersion;
    this.softwareVersion = softwareVersion;
    this.capabilities = capabilities;
  }

  /**
   * 从已有的StorageInfo构造NamespaceInfo，自动获取版本信息
   * @param storage 已有的存储信息对象
   */
  public NamespaceInfo(StorageInfo storage) {
    super(storage);
    if (storage instanceof NamespaceInfo) {
      this.capabilities = ((NamespaceInfo)storage).capabilities;
      this.blockPoolID = ((NamespaceInfo)storage).blockPoolID;
    } else {
      this.capabilities = CAPABILITIES_SUPPORTED;
    }
    this.buildVersion = Storage.getBuildVersion();
    this.softwareVersion = VersionInfo.getVersion();
    if (storage instanceof NNStorage) {
      this.blockPoolID = ((NNStorage)storage).getBlockPoolID();
    } else {
      this.blockPoolID = null;
    }

  }

  /**
   * 从已有的StorageInfo构造NamespaceInfo，并指定HA状态
   * @param storage 已有的存储信息对象
   * @param st 当前HA服务状态
   */
  public NamespaceInfo(StorageInfo storage, HAServiceState st) {
    this(storage);
    this.state = st;
  }

  /**
   * 构造NamespaceInfo，自动填充本地版本信息，默认开启所有能力
   * @param nsID 命名空间ID
   * @param clusterID 集群ID
   * @param bpID 块池ID
   * @param cT 命名空间创建时间戳
   */
  public NamespaceInfo(int nsID, String clusterID, String bpID, 
      long cT) {
    this(nsID, clusterID, bpID, cT, Storage.getBuildVersion(),
        VersionInfo.getVersion());
  }

  /**
   * 构造NamespaceInfo，自动填充本地版本信息，指定HA状态
   * @param nsID 命名空间ID
   * @param clusterID 集群ID
   * @param bpID 块池ID
   * @param cT 命名空间创建时间戳
   * @param st 当前HA服务状态
   */
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, HAServiceState st) {
    this(nsID, clusterID, bpID, cT, Storage.getBuildVersion(),
        VersionInfo.getVersion());
    this.state = st;
  }
  
  /**
   * 获取能力位图
   * @return 能力位图
   */
  public long getCapabilities() {
    return capabilities;
  }

  /**
   * 设置能力位图，仅用于测试
   * @param capabilities 能力位图
   */
  @VisibleForTesting
  public void setCapabilities(long capabilities) {
    this.capabilities = capabilities;
  }

  /**
   * 设置HA状态，仅用于测试
   * @param state HA服务状态
   */
  @VisibleForTesting
  public void setState(HAServiceState state) {
    this.state = state;
  }

  /**
   * 检查指定能力是否被支持
   * @param capability 要检查的能力枚举
   * @return true表示支持，false表示不支持
   */
  public boolean isCapabilitySupported(Capability capability) {
    Preconditions.checkArgument(capability != Capability.UNKNOWN,
        "cannot test for unknown capability");
    long mask = capability.getMask();
    return (capabilities & mask) == mask;
  }

  /**
   * 获取构建版本号
   * @return 构建版本字符串
   */
  public String getBuildVersion() {
    return buildVersion;
  }

  /**
   * 获取块池ID
   * @return 块池ID字符串
   */
  public String getBlockPoolID() {
    return blockPoolID;
  }
  
  /**
   * 获取软件版本号
   * @return 软件版本字符串
   */
  public String getSoftwareVersion() {
    return softwareVersion;
  }

  /**
   * 获取当前HA服务状态
   * @return HA服务状态枚举
   */
  public HAServiceState getState() {
    return state;
  }

  /**
   * 设置集群ID
   * @param clusterID 集群ID字符串
   */
  public void setClusterID(String clusterID) {
    this.clusterID = clusterID;
  }

  /**
   * 设置块池ID
   * @param blockPoolID 块池ID字符串
   */
  public void setBlockPoolID(String blockPoolID) {
    this.blockPoolID = blockPoolID;
  }

  @Override
  public String toString(){
    return super.toString() + ";bpid=" + blockPoolID;
  }

  /**
   * 验证存储信息与当前命名空间信息是否一致，不一致抛出异常
   * 用于NameNode启动时检查元数据存储是否和命名空间信息匹配
   * @param storage 待验证的NameNode存储对象
   * @throws IOException 信息不一致时抛出异常
   */
  public void validateStorage(NNStorage storage) throws IOException {
    if (layoutVersion != storage.getLayoutVersion() ||
        namespaceID != storage.getNamespaceID() ||
        cTime != storage.cTime ||
        !clusterID.equals(storage.getClusterID()) ||
        !blockPoolID.equals(storage.getBlockPoolID())) {
      throw new IOException("Inconsistent namespace information:\n" +
          "NamespaceInfo has:\n" +
          "LV=" + layoutVersion + ";" +
          "NS=" + namespaceID + ";" +
          "cTime=" + cTime + ";" +
          "CID=" + clusterID + ";" +
          "BPID=" + blockPoolID +
          ".\nStorage has:\n" +
          "LV=" + storage.getLayoutVersion() + ";" +
          "NS=" + storage.getNamespaceID() + ";" +
          "cTime=" + storage.getCTime() + ";" +
          "CID=" + storage.getClusterID() + ";" +
          "BPID=" + storage.getBlockPoolID() + ".");
    }
  }
}