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
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * HDFS Web服务JSON序列化工具类
 * 提供将各类HDFS内部对象转换为JSON字符串的能力，供WebHDFS REST API返回响应使用
 */
public class JsonUtil {
  private static final Object[] EMPTY_OBJECT_ARRAY = {};

  // 复用ObjectMapper实例提升性能，ObjectMapper线程安全，WebHDFS无重入调用场景
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * 将安全令牌对象转换为JSON字符串
   * @param token 安全令牌对象
   * @return 序列化后的JSON字符串
   * @throws IOException 序列化失败时抛出异常
   */
  public static String toJsonString(final Token<? extends TokenIdentifier> token
      ) throws IOException {
    return toJsonString(Token.class, toJsonMap(token));
  }

  private static Map<String, Object> toJsonMap(
      final Token<? extends TokenIdentifier> token) throws IOException {
    if (token == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("urlString", token.encodeToUrlString());
    return m;
  }

  /**
   * 将异常对象转换为JSON字符串，供REST API返回错误信息
   * @param e 异常对象
   * @return 包含异常信息的JSON字符串
   */
  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("exception", e.getClass().getSimpleName());
    m.put("message", e.getMessage());
    m.put("javaClassName", e.getClass().getName());
    return toJsonString(RemoteException.class, m);
  }

  private static String toJsonString(final Class<?> clazz, final Object value) {
    return toJsonString(clazz.getSimpleName(), value);
  }

  /**
   * 将单个键值对转换为JSON字符串
   * @param key 键名称
   * @param value 值对象
   * @return 序列化后的JSON字符串
   */
  public static String toJsonString(final String key, final Object value) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put(key, value);
    try {
      return MAPPER.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /**
   * 将权限对象转换为八进制权限字符串
   * @param permission HDFS权限对象
   * @return 八进制权限字符串
   */
  private static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /**
   * 将HDFS文件状态对象转换为JSON字符串
   * @param status HDFS文件状态对象
   * @param includeType 是否包装类型信息
   * @return 序列化后的JSON字符串
   */
  public static String toJsonString(final HdfsFileStatus status,
      boolean includeType) {
    if (status == null) {
      return null;
    }
    final Map<String, Object> m = toJsonMap(status);
    try {
      return includeType ?
          toJsonString(FileStatus.class, m) : MAPPER.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  private static Map<String, Object> toJsonMap(HdfsFileStatus status) {
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("pathSuffix", status.getLocalName());
    m.put("type", WebHdfsConstants.PathType.valueOf(status));
    if (status.isSymlink()) {
      m.put("symlink", DFSUtilClient.bytes2String(status.getSymlinkInBytes()));
    }
    m.put("length", status.getLen());
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    FsPermission perm = status.getPermission();
    m.put("permission", toString(perm));
    if (status.hasAcl()) {
      m.put("aclBit", true);
    }
    if (status.isEncrypted()) {
      m.put("encBit", true);
    }
    if (status.isErasureCoded()) {
      m.put("ecBit", true);
      if (status.getErasureCodingPolicy() != null) {
        // 保持向后兼容性
        m.put("ecPolicy", status.getErasureCodingPolicy().getName());
        // 用于通过WebHDFS重建HdfsFileStatus对象
        m.put("ecPolicyObj", getEcPolicyAsMap(status.getErasureCodingPolicy()));
      }
    }
    if (status.isSnapshotEnabled()) {
      m.put("snapshotEnabled", status.isSnapshotEnabled());
    }

    m.put("accessTime", status.getAccessTime());
    m.put("modificationTime", status.getModificationTime());
    m.put("blockSize", status.getBlockSize());
    m.put("replication", status.getReplication());
    m.put("fileId", status.getFileId());
    m.put("childrenNum", status.getChildrenNum());
    m.put("storagePolicy", status.getStoragePolicy());
    return m;
  }

  /**
   * 将纠删码策略转换为不可变Map，用于JSON序列化
   * @param ecPolicy 纠删码策略对象
   * @return 包含策略信息的不可变Map
   */
  public static Map<String, Object> getEcPolicyAsMap(
      final ErasureCodingPolicy ecPolicy) {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("name", ecPolicy.getName())
        .put("cellSize", ecPolicy.getCellSize())
        .put("numDataUnits", ecPolicy.getNumDataUnits())
        .put("numParityUnits", ecPolicy.getNumParityUnits())
        .put("codecName", ecPolicy.getCodecName())
        .put("id", ecPolicy.getId())
        .put("extraOptions", ecPolicy.getSchema().getExtraOptions());
    return builder.build();

  }

  /**
   * 将扩展块对象转换为JSON Map
   * @param extendedblock 扩展块对象
   * @return 包含块信息的Map
   */
  private static Map<String, Object> toJsonMap(final ExtendedBlock extendedblock) {
    if (extendedblock == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("blockPoolId", extendedblock.getBlockPoolId());
    m.put("blockId", extendedblock.getBlockId());
    m.put("numBytes", extendedblock.getNumBytes());
    m.put("generationStamp", extendedblock.getGenerationStamp());
    return m;
  }

  /**
   * 将DataNode信息对象转换为JSON Map
   * @param datanodeinfo DataNode信息对象
   * @return 包含DataNode信息的Map
   */
  static Map<String, Object> toJsonMap(final DatanodeInfo datanodeinfo) {
    if (datanodeinfo == null) {
      return null;
    }

    // TODO: Fix storageID
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("ipAddr", datanodeinfo.getIpAddr());
    // 'name' 等价于 ipAddr:xferPort，旧版客户端(1.x, 0.23.x)依赖此字段
    m.put("name", datanodeinfo.getXferAddr());
    m.put("hostName", datanodeinfo.getHostName());
    m.put("storageID", datanodeinfo.getDatanodeUuid());
    m.put("xferPort", datanodeinfo.getXferPort());
    m.put("infoPort", datanodeinfo.getInfoPort());
    m.put("infoSecurePort", datanodeinfo.getInfoSecurePort());
    m.put("ipcPort", datanodeinfo.getIpcPort());

    m.put("capacity", datanodeinfo.getCapacity());
    m.put("dfsUsed", datanodeinfo.getDfsUsed());
    m.put("remaining", datanodeinfo.getRemaining());
    m.put("blockPoolUsed", datanodeinfo.getBlockPoolUsed());
    m.put("cacheCapacity", datanodeinfo.getCacheCapacity());
    m.put("cacheUsed", datanodeinfo.getCacheUsed());
    m.put("lastUpdate", datanodeinfo.getLastUpdate());
    m.put("lastUpdateMonotonic", datanodeinfo.getLastUpdateMonotonic());
    m.put("xceiverCount", datanodeinfo.getXceiverCount());
    m.put("networkLocation", datanodeinfo.getNetworkLocation());
    m.put("adminState", datanodeinfo.getAdminState().name());
    if (datanodeinfo.getUpgradeDomain() != null) {
      m.put("upgradeDomain", datanodeinfo.getUpgradeDomain());
    }
    m.put("lastBlockReportTime", datanodeinfo.getLastBlockReportTime());
    m.put("lastBlockReportMonotonic",
        datanodeinfo.getLastBlockReportMonotonic());
    return m;
  }

  /**
   * 将DataNode信息数组转换为JSON数组
   * @param array DataNode信息数组
   * @return 转换后的JSON数组
   */
  private static Object[] toJsonArray(final DatanodeInfo[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.length];
      for(int i = 0; i < array.length; i++) {
        a[i] = toJsonMap(array[i]);
      }
      return a;
    }
  }

  /**
   * 将存储类型数组转换为JSON数组
   * @param array 存储类型数组
   * @return 转换后的JSON数组
   */
  private static Object[] toJsonArray(final StorageType[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.length];
      for(int i = 0; i < array.length; i++) {
        a[i] = array[i];
      }
      return a;
    }
  }

  /**
   * 已定位块对象转换为JSON Map
   * @param locatedblock 已定位块对象
   * @return 包含块位置信息的Map
   * @throws IOException 序列化失败时抛出异常
   */
  private static Map<String, Object> toJsonMap(final LocatedBlock locatedblock
      ) throws IOException {
    if (locatedblock == null) {
      return null;
    }
 
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("blockToken", toJsonMap(locatedblock.getBlockToken()));
    m.put("isCorrupt", locatedblock.isCorrupt());
    m.put("startOffset", locatedblock.getStartOffset());
    m.put("block", toJsonMap(locatedblock.getBlock()));
    m.put("storageTypes", toJsonArray(locatedblock.getStorageTypes()));
    m.put("locations", toJsonArray(locatedblock.getLocations()));
    m.put("cachedLocations", toJsonArray(locatedblock.getCachedLocations()));
    return m;
  }

  private static Map<String, Object> toJson(final DirectoryListing listing)
      throws IOException {
    final Map<String, Object> m = new TreeMap<>();
    // 将FileStatus数组序列化为FileStatuses Map
    m.put("partialListing", toJsonMap(listing.getPartialListing()));
    // 剩余条目数
    m.put("remainingEntries", listing.getRemainingEntries());

    return m;
  }

  /**
   * 将目录列表对象转换为JSON字符串
   * @param listing 目录列表对象
   * @return 序列化后的JSON字符串
   * @throws IOException 序列化失败时抛出异常
   */
  public static String toJsonString(final DirectoryListing listing) throws
      IOException {

    if (listing == null) {
      return null;
    }
    return toJsonString(DirectoryListing.class, toJson(listing));
  }

  private static Map<String, Object> toJsonMap(HdfsFileStatus[] statuses) throws
      IOException {
    if (statuses == null) {
      return null;
    }

    final Map<String, Object> fileStatuses = new TreeMap<>();
    final Map<String, Object> fileStatus = new TreeMap<>();
    fileStatuses.put("FileStatuses", fileStatus);
    final Object[] array = new Object[statuses.length];
    fileStatus.put("FileStatus", array);
    for (int i = 0; i < statuses.length; i++) {
      array[i] = toJsonMap(statuses[i]);
    }

    return fileStatuses;
  }

  /**
   * 将已定位块列表转换为JSON数组
   * @param array 已定位块列表
   * @return 转换后的JSON数组
   * @throws IOException 序列化失败时抛出异常
   */
  private static Object[] toJsonArray(final List<LocatedBlock> array
      ) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.size()];
      for(int i = 0; i < array.size(); i++) {
        a[i] = toJsonMap(array.get(i));
      }
      return a;
    }
  }

  /**
   * 将文件块位置信息对象转换为JSON字符串
   * @param locatedblocks 文件块位置信息对象
   * @return 序列化后的JSON字符串
   * @throws IOException 序列化失败时抛出异常
   */
  public static String toJsonString(final LocatedBlocks locatedblocks
      ) throws IOException {
    if (locatedblocks == null) {
      return null;
    }

    final Map<String, Object> m = toJsonMap(locatedblocks);
    return toJsonString(LocatedBlocks.class, m);
  }

  /**
   * 将文件块位置信息对象转换为Map
   * @param locatedblocks 文件块位置信息对象
   * @return 包含块位置信息的Map
   * @throws IOException 序列化失败时抛出异常
   */
  public static Map<String, Object> toJsonMap(final LocatedBlocks locatedblocks)
      throws IOException {
    if (locatedblocks == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("fileLength", locatedblocks.getFileLength());
    m.put("isUnderConstruction", locatedblocks.isUnderConstruction());

    m.put("locatedBlocks", toJsonArray(locatedblocks.getLocatedBlocks()));
    m.put("lastLocatedBlock", toJsonMap(locatedblocks.getLastLocatedBlock()));
    m.put("isLastBlockComplete", locatedblocks.isLastBlockComplete());
    return m;
  }

  /**
   * 将目录内容摘要对象转换为JSON字符串
   * @param contentsummary 目录内容摘要对象
   * @return 序列化后的JSON字符串
   */
  public static String toJsonString(final ContentSummary contentsummary) {
    if (contentsummary == null) {
      return null;
    }

    final Map<String, Object> m = new Tree