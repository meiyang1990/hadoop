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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/StorageInfo.java
 * <p>
 * HDFS存储信息基础类，负责管理存储的元数据信息，包括布局版本、命名空间ID、集群ID等核心标识信息，
 * 提供从VERSION文件读取和解析存储信息的能力，是NameNode和DataNode存储管理的基础抽象。
 */
@InterfaceAudience.Private
public class StorageInfo {
  public int   layoutVersion;   // 存储数据的布局版本
  public int   namespaceID;     // 文件系统的命名空间ID
  public String clusterID;      // 集群ID
  public long  cTime;           // 文件系统状态创建时间

  protected final NodeType storageType; // 使用该存储的节点类型（NameNode/DataNode
  
  protected static final String STORAGE_FILE_VERSION    = "VERSION";

  /**
   * 构造方法，使用默认值初始化存储信息
   * @param type 节点类型
   */
  public StorageInfo(NodeType type) {
    this(0, 0, "", 0L, type);
  }

  /**
   * 构造方法，使用指定值初始化存储信息
   * @param layoutV 存储布局版本
   * @param nsID 命名空间ID
   * @param cid 集群ID
   * @param cT 文件系统创建时间
   * @param type 节点类型
   */
  public StorageInfo(int layoutV, int nsID, String cid, long cT, NodeType type) {
    layoutVersion = layoutV;
    clusterID = cid;
    namespaceID = nsID;
    cTime = cT;
    storageType = type;
  }
  
  /**
   * 拷贝构造方法，从已有StorageInfo复制生成新实例
   * @param from 要复制的源StorageInfo对象
   */
  public StorageInfo(StorageInfo from) {
    this(from.layoutVersion, from.namespaceID, from.clusterID, from.cTime,
        from.storageType);
  }

  /**
   * 获取存储数据的布局版本
   * @return 布局版本号
   */
  public int    getLayoutVersion(){ return layoutVersion; }

  /**
   * 获取文件系统的命名空间ID
   * <p>
   * 命名空间ID在文件系统格式化时分配，之后永不改变，被所有文件系统组件共享
   * @return 命名空间ID
   */
  public int    getNamespaceID()  { return namespaceID; }

  /**
   * 获取文件系统所属集群ID
   * @return 集群ID字符串
   */
  public String    getClusterID()  { return clusterID; }
  
  /**
   * 获取文件系统状态创建时间
   * <p>
   * 在升级过程中会被修改
   * @return 创建时间戳
   */
  public long   getCTime()        { return cTime; }
  
  /**
   * 从另一个StorageInfo对象复制所有存储信息到当前对象
   * @param from 源StorageInfo对象
   */
  public void   setStorageInfo(StorageInfo from) {
    layoutVersion = from.layoutVersion;
    clusterID = from.clusterID;
    namespaceID = from.namespaceID;
    cTime = from.cTime;
  }

  /**
   * 检查当前存储版本是否支持HDFS联邦特性
   * @param map 特性版本对应关系表
   * @return true表示支持，false表示不支持
   */
  public boolean versionSupportsFederation(
      Map<Integer, SortedSet<LayoutFeature>> map) {
    return LayoutVersion.supports(map, LayoutVersion.Feature.FEDERATION,
        layoutVersion);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("lv=").append(layoutVersion).append(";cid=").append(clusterID)
    .append(";nsid=").append(namespaceID).append(";c=").append(cTime);
    return sb.toString();
  }

  /**
   * 将存储信息属性转换为Map形式的字符串表示
   * @return 存储信息的字符串表示
   */
  public String toMapString() {
    Map<String, Object> storageInfo = new HashMap<>();
    storageInfo.put("LayoutVersion", layoutVersion);
    storageInfo.put("ClusterId", clusterID);
    storageInfo.put("NamespaceId", namespaceID);
    storageInfo.put("CreationTime", cTime);
    return storageInfo.toString();
  }
  
  /**
   * 将存储信息转换为冒号分隔的字符串
   * @return 冒号分隔的存储信息字符串
   */
  public String toColonSeparatedString() {
    return Joiner.on(":").join(
        layoutVersion, namespaceID, cTime, clusterID);
  }
  
  /**
   * 从冒号分隔的字符串中提取命名空间ID
   * @param in 冒号分隔的输入字符串
   * @return 命名空间ID
   */
  public static int getNsIdFromColonSeparatedString(String in) {
    return Integer.parseInt(in.split(":")[1]);
  }
  
  /**
   * 从冒号分隔的字符串中提取集群ID
   * @param in 冒号分隔的输入字符串
   * @return 集群ID字符串
   */
  public static String getClusterIdFromColonSeparatedString(String in) {
    return in.split(":")[3];
  }
  
  /**
   * 从指定存储目录的VERSION文件中读取存储属性信息
   * @param sd 存储目录对象
   * @throws IOException 读取或解析失败时抛出异常
   */
  public void readProperties(StorageDirectory sd) throws IOException {
    Properties props = readPropertiesFile(sd.getVersionFile());
    setFieldsFromProperties(props, sd);
  }
  
  /**
   * 从指定存储目录的previous/VERSION文件中读取升级前的存储属性信息
   * @param sd 存储目录对象
   * @throws IOException 读取或解析失败时抛出异常
   */
  public void readPreviousVersionProperties(StorageDirectory sd)
      throws IOException {
    Properties props = readPropertiesFile(sd.getPreviousVersionFile());
    setFieldsFromProperties(props, sd);
  }
  
  /**
   * 从Properties对象中提取通用存储字段赋值到当前对象，子类可重写添加额外字段的提取逻辑
   * @param props 从VERSION文件加载的属性对象
   * @param sd 存储目录对象
   * @throws IOException 属性不合法时抛出异常
   */
  protected void setFieldsFromProperties(
      Properties props, StorageDirectory sd) throws IOException {
    if (props == null) {
      return;
    }
    setLayoutVersion(props, sd);
    setNamespaceID(props, sd);
    setcTime(props, sd);
    setClusterId(props, layoutVersion, sd);
    checkStorageType(props, sd);
  }
  
  /**
   * 从Properties中验证并验证存储类型是否匹配当前节点类型
   * @param props 加载后的属性对象
   * @param sd 存储目录对象
   * @throws InconsistentFSStateException 存储类型不匹配时抛出异常
   */
  protected void checkStorageType(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    if (storageType == null) { // 不需要验证存储类型
      return;
    }
    NodeType type = NodeType.valueOf(getProperty(props, sd, "storageType"));
    if (!storageType.equals(type)) {
      throw new InconsistentFSStateException(sd.root,
          "Incompatible node types: storageType=" + storageType
          + " but StorageDirectory type=" + type);
    }
  }
  
  /**
   * 从Properties中提取并设置创建时间
   * @param props 加载后的属性对象
   * @param sd 存储目录对象
   * @throws InconsistentFSStateException 属性缺失时抛出异常
   */
  protected void setcTime(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    cTime = Long.parseLong(getProperty(props, sd, "cTime"));
  }

  /**
   * 从Properties中提取并验证集群ID
   * @param props 加载后的属性对象
   * @param layoutVersion 当前存储布局版本
   * @param sd 存储目录对象
   * @throws InconsistentFSStateException 集群ID不兼容时抛出异常
   */
  protected void setClusterId(Properties props, int layoutVersion,
      StorageDirectory sd) throws InconsistentFSStateException {
    // 仅在支持联邦的版本中读取并设置集群ID
    if (LayoutVersion.supports(getServiceLayoutFeatureMap(),
        Feature.FEDERATION, layoutVersion)) {
      String cid = getProperty(props, sd, "clusterID");
      if (!(clusterID.equals("") || cid.equals("") || clusterID.equals(cid))) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "cluster Id is incompatible with others.");
      }
      clusterID = cid;
    }
  }
  
  /**
   * 从Properties中提取并验证布局版本
   * @param props 加载后的属性对象
   * @param sd 存储目录对象
   * @throws IncorrectVersionException 版本高于当前服务不兼容时抛出异常
   * @throws InconsistentFSStateException 属性缺失时抛出异常
   */
  protected void setLayoutVersion(Properties props, StorageDirectory sd)
      throws IncorrectVersionException, InconsistentFSStateException {
    int lv = Integer.parseInt(getProperty(props, sd, "layoutVersion"));
    if (lv < getServiceLayoutVersion()) { // 未来版本号高于当前服务版本，不兼容
      throw new IncorrectVersionException(getServiceLayoutVersion(), lv,
          "storage directory " + sd.root.getAbsolutePath());
    }
    layoutVersion = lv;
  }
  
  /**
   * 从Properties中提取并验证命名空间ID
   * @param props 加载后的属性对象
   * @param sd 存储目录对象
   * @throws InconsistentFSStateException 命名空间ID不兼容时抛出异常
   */
  protected void setNamespaceID(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    int nsId = Integer.parseInt(getProperty(props, sd, "namespaceID"));
    if (namespaceID != 0 && nsId != 0 && namespaceID != nsId) {
      throw new InconsistentFSStateException(sd.root,
          "namespaceID is incompatible with others.");
    }
    namespaceID = nsId;
  }

  /**
   * 设置当前服务的布局版本
   * @param lv 布局版本号
   */
  public void setServiceLayoutVersion(int lv) {
    this.layoutVersion = lv;
  }

  /**
   * 获取当前节点类型对应的当前布局版本
   * @return 当前布局版本号
   */
  public int getServiceLayoutVersion() {
    return storageType == NodeType.DATA_NODE
        ? DataNodeLayoutVersion.getCurrentLayoutVersion()
        : HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
  }

  /**
   * 获取当前节点类型对应的布局特性版本映射表
   * @return 布局特性映射表，key为版本号，value为对应支持的特性集合
   */
  public Map<Integer, SortedSet<LayoutFeature>> getServiceLayoutFeatureMap() {
    return storageType == NodeType.DATA_NODE? DataNodeLayoutVersion.FEATURES
        : NameNodeLayoutVersion.FEATURES;
  }
  
  /**
   * 从Properties中获取指定属性，属性不存在则抛出异常
   * @param props Properties对象
   * @param sd 存储目录对象
   * @param name 属性名
   * @return 属性值字符串
   * @throws InconsistentFSStateException 属性不存在时抛出异常
   */
  protected static String getProperty(Properties props, StorageDirectory sd,
      String name) throws InconsistentFSStateException {
    String property = props.getProperty(name);
    if (property == null) {
      throw new InconsistentFSStateException(sd.root, "file "
          + STORAGE_FILE_VERSION + " has " + name + " missing.");
    }
    return property;
  }

  /**
   * 从指定文件读取属性，加载为Properties对象
   * @param from 要读取的文件对象
   * @return 加载后的Properties对象，输入文件为null则返回null
   * @throws IOException 读取失败时抛出异常
   */
  public static Properties readPropertiesFile(File from) throws IOException {
    if (from == null) {
      return null;
    }
    RandomAccessFile file = new RandomAccessFile(from, "rws");
    FileInputStream in = null;
    Properties props = new Properties();
    try {
      in = new FileInputStream(file.getFD());
      file.seek(0);
      props.load(in);
    } finally {
      if (in != null) {
      in.close();
    }
      file.close();
    }
    return props;
  }
}