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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;


/**
 * 封装DataNode存储目录的URI和存储介质类型信息。
 * 如果未指定存储介质类型，默认类型为DISK。
 * 用于表示DataNode管理的单个存储位置，支持不同存储介质类型配置和解析。
 */
@InterfaceAudience.Private
public class StorageLocation
    implements Checkable<StorageLocation.CheckContext, VolumeCheckResult>,
               Comparable<StorageLocation> {
  private final StorageType storageType;
  private final URI baseURI;
  /** 匹配带存储类型前缀的存储位置的正则表达式，例如 [Disk]/storages/storage1/ */
  private static final Pattern STORAGE_LOCATION_REGEX =
      Pattern.compile("^\\[(\\w*)\\](.+)$");

  /** 匹配存储容量比例配置的正则表达式，用于同盘分层存储配置，例如 [0.3]/disk1/archive/ */
  private static final Pattern CAPACITY_RATIO_REGEX =
      Pattern.compile("^\\[([0-9.]*)\\](.+)$");

  /**
   * 构造函数，创建存储位置对象，内部使用。
   * @param storageType 存储介质类型
   * @param uri 存储位置URI
   */
  private StorageLocation(StorageType storageType, URI uri) {
    this.storageType = storageType;
    // 如果是本地文件路径，统一标准化URI格式
    if (uri.getScheme() == null || uri.getScheme().equals("file")) {
      // 确保所有指向本地文件的URI格式统一
      uri = normalizeFileURI(uri);
    }
    baseURI = uri;
  }

  /**
   * 标准化本地文件URI，确保格式一致。
   * @param uri 原始本地文件URI
   * @return 标准化后的URI，去除末尾斜杠
   */
  public static URI normalizeFileURI(URI uri) {
    try {
      File uriFile = new File(uri.getPath());
      String uriStr = uriFile.toURI().normalize().toString();
      if (uriStr.endsWith("/")) {
        uriStr = uriStr.substring(0, uriStr.length() - 1);
      }
      return new URI(uriStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
              "URI: " + uri + " is not in the expected format");
    }
  }

  /**
   * 获取该存储位置的存储介质类型。
   * @return 存储类型枚举
   */
  public StorageType getStorageType() {
    return this.storageType;
  }

  /**
   * 获取该存储位置的基础URI。
   * @return 存储位置URI
   */
  public URI getUri() {
    return baseURI;
  }

  /**
   * 获取标准化后的URI。
   * @return 标准化后的URI
   */
  public URI getNormalizedUri() {
    return baseURI.normalize();
  }

  /**
   * 检查当前存储位置是否匹配给定的存储目录。
   * @param sd 存储目录对象
   * @return 是否匹配
   * @throws IOException IO异常
   */
  public boolean matchesStorageDirectory(StorageDirectory sd)
      throws IOException {
    return this.equals(sd.getStorageLocation());
  }

  /**
   * 检查当前存储位置是否匹配指定块池的存储目录，处理PROVIDED存储类型特殊逻辑。
   * @param sd 存储目录对象
   * @param bpid 块池ID
   * @return 是否匹配
   * @throws IOException IO异常
   */
  public boolean matchesStorageDirectory(StorageDirectory sd,
      String bpid) throws IOException {
    // 如果两者都是PROVIDED类型，直接比较对象
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED &&
        storageType == StorageType.PROVIDED) {
      return matchesStorageDirectory(sd);
    }
    // 只要一方是PROVIDED另一方不是，就不匹配（系统只能存在一个PROVIDED存储目录）
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED ||
        storageType == StorageType.PROVIDED) {
      // 只能存在一个PROVIDED存储目录，因此不可能匹配
      return false;
    }
    // 对比块池目录的标准化URI判断是否匹配
    return this.getBpURI(bpid, Storage.STORAGE_DIR_CURRENT).normalize()
        .equals(sd.getRoot().toURI().normalize());
  }

  /**
   * 解析存储位置字符串，生成StorageLocation对象，支持可选的存储类型前缀。
   * 存储类型部分不区分大小写。
   *
   * @param rawLocation 原始位置字符串，格式为 [type]uri，其中[type]可选
   * @return 解析成功返回StorageLocation对象，失败返回null，不抛出异常
   */
  public static StorageLocation parse(String rawLocation)
      throws IOException, SecurityException {
    Matcher matcher = STORAGE_LOCATION_REGEX.matcher(rawLocation);
    StorageType storageType = StorageType.DEFAULT;
    String location = rawLocation;

    // 如果匹配带存储类型的格式，提取类型和路径
    if (matcher.matches()) {
      String classString = matcher.group(1);
      location = matcher.group(2).trim();
      if (!classString.isEmpty()) {
        storageType =
            StorageType.valueOf(StringUtils.toUpperCase(classString));
      }
    }
    // 使用Path.toURI保证路径格式一致（"/a/b"和"/a/b/"归一化后相同）
    return new StorageLocation(storageType, new Path(location).toUri());
  }

  /**
   * 解析容量比例配置字符串，提取每个存储卷的容量比例配置。
   * 用于同盘分层存储场景，同一个磁盘上划分不同比例给不同存储类型使用。
   *
   * @param capacityRatioConf 容量比例配置字符串
   * @return 存储卷URI到容量比例的映射表
   * @throws SecurityException 格式错误或比例不在0-1范围时抛出异常
   */
  public static Map<URI, Double> parseCapacityRatio(String capacityRatioConf)
      throws SecurityException {
    Map<URI, Double> result = new HashMap<>();
    // 去除所有空白字符
    capacityRatioConf = capacityRatioConf.replaceAll("\\s", "");
    if (capacityRatioConf.isEmpty()) {
      return result;
    }
    // 按逗号分割多个配置项
    String[] capacityRatios = capacityRatioConf.split(",");
    for (String ratio : capacityRatios) {
      Matcher matcher = CAPACITY_RATIO_REGEX.matcher(ratio);
      if (matcher.matches()) {
        String capacityString = matcher.group(1).trim();
        String location = matcher.group(2).trim();
        double capacityRatio = Double.parseDouble(capacityString);
        // 验证比例范围必须在0到1之间
        if (capacityRatio > 1 || capacityRatio < 0) {
          throw new IllegalArgumentException("Capacity ratio" + capacityRatio
              + " is not between 0 to 1: " + ratio);
        }
        result.put(new Path(location).toUri(), capacityRatio);
      } else {
        throw new IllegalArgumentException(
            "Capacity ratio config is not with correct format: "
                + capacityRatioConf
        );
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + baseURI.normalize();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StorageLocation)) {
      return false;
    }
    int comp = compareTo((StorageLocation) obj);
    return comp == 0;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public int compareTo(StorageLocation obj) {
    if (obj == this) {
      return 0;
    } else if (obj == null) {
      return -1;
    }

    StorageLocation otherStorage = (StorageLocation) obj;
    // 优先比较标准化后的URI
    if (this.getNormalizedUri() != null &&
        otherStorage.getNormalizedUri() != null) {
      return this.getNormalizedUri().compareTo(
          otherStorage.getNormalizedUri());
    } else if (this.getNormalizedUri() == null &&
        otherStorage.getNormalizedUri() == null) {
      // URI都为空时比较存储类型
      return this.storageType.compareTo(otherStorage.getStorageType());
    } else if (this.getNormalizedUri() == null) {
      // 当前对象URI为空，排在后面
      return -1;
    } else {
      // 对方对象URI为空，当前对象排在前面
      return 1;
    }

  }

  /**
   * 获取指定块池的存储目录URI。
   * @param bpid 块池ID
   * @param currentStorageDir 当前存储目录名称
   * @return 块池存储目录URI，如果路径非法返回null
   */
  public URI getBpURI(String bpid, String currentStorageDir) {
    try {
      File localFile = new File(getUri());
      return new File(new File(localFile, currentStorageDir), bpid).toURI();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * 在该存储位置上创建指定块池的物理目录。
   *
   * @param blockPoolID 块池ID
   * @param conf Hadoop配置对象
   * @throws IOException 创建目录或权限检查失败时抛出
   */
  public void makeBlockPoolDir(String blockPoolID,
      Configuration conf) throws IOException {

    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    // PROVIDED类型存储不需要创建目录，由外部提供
    if (storageType == StorageType.PROVIDED) {
      // 跳过PROVIDED存储类型的目录创建
      Storage.LOG.info("Skipping creating directory for block pool "
          + blockPoolID + " for PROVIDED storage location " + this);
      return;
    }

    LocalFileSystem localFS = FileSystem.getLocal(conf);
    // 从配置获取数据目录权限，使用默认值兜底
    FsPermission permission = new FsPermission(conf.get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    File data = new File(getBpURI(blockPoolID, Storage.STORAGE_DIR_CURRENT));
    try {
      // 检查目录合法性，不存在则创建，检查权限
      DiskChecker.checkDir(localFS, new Path(data.toURI()), permission);
    } catch (IOException e) {
      DataStorage.LOG.warn("Invalid directory in: " + data.getCanonicalPath() +
          ": " + e.getMessage());
      throw e;
    }
  }

  @Override  // Checkable
  /**
   * 检查该存储位置的健康状态，验证目录存在性和权限。
   * @param context 检查上下文，包含文件系统和期望权限
   * @return 健康检查结果
   * @throws IOException 检查失败抛出IO异常
   */
  public VolumeCheckResult check(CheckContext context) throws IOException {
    // PROVIDED存储默认认为健康，仅检查本地存储
    if (storageType != StorageType.PROVIDED) {
      DiskChecker.checkDir(
          context.localFileSystem,
          new Path(baseURI),
          context.expectedPermission);
    }
    return VolumeCheckResult.HEALTHY;
  }

  /**
   * 存储位置健康检查的上下文参数，持有检查所需的文件系统和权限信息。
   */
  public static final class CheckContext {
    private final LocalFileSystem localFileSystem;
    private final FsPermission expectedPermission;

    /**
     * 构造检查上下文对象。
     * @param localFileSystem 本地文件系统对象
     * @param expectedPermission 期望的目录权限
     */
    public CheckContext(LocalFileSystem localFileSystem,
                        FsPermission expectedPermission) {
      this.localFileSystem = localFileSystem;
      this.expectedPermission = expectedPermission;
    }
  }
}