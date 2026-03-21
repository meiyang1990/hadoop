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

package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYPASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.AuthFilterInitializer;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;

/**
 * HDFS 工具类，提供 HDFS 核心功能相关的通用工具方法
 * 包括地址解析、DataNode排序、路径处理、配置处理、加密、HTTP服务器构建等能力
 */
@InterfaceAudience.Private
public class DFSUtil {
  public static final Logger LOG =
      LoggerFactory.getLogger(DFSUtil.class.getName());
  
  private DFSUtil() { /* Hidden constructor */ }
  
  // 线程本地安全随机数生成器，避免多线程竞争
  private static final ThreadLocal<SecureRandom> SECURE_RANDOM = new ThreadLocal<SecureRandom>() {
    @Override
    protected SecureRandom initialValue() {
      return new SecureRandom();
    }
  };

  /** @return a pseudo secure random number generator. */
  public static SecureRandom getSecureRandom() {
    return SECURE_RANDOM.get();
  }

  /**
   * 按DataNode服务状态排序的比较器，退役/维护节点排到末尾
   */
  public static class ServiceComparator implements Comparator<DatanodeInfo> {
    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      // 已退役节点排到末尾
      if (a.isDecommissioned()) {
        return b.isDecommissioned() ? 0 : 1;
      } else if (b.isDecommissioned()) {
        return -1;
      }

      // 退役中节点排在已退役节点之前
      if (a.isDecommissionInProgress()) {
        return b.isDecommissionInProgress() ? 0 : 1;
      } else if (b.isDecommissionInProgress()) {
        return -1;
      }

      // 进入维护中节点排在正常节点之后
      if (a.isEnteringMaintenance()) {
        return b.isEnteringMaintenance() ? 0 : 1;
      } else if (b.isEnteringMaintenance()) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  /**
   * 扩展排序比较器，在服务状态基础上增加了慢节点、 stale 节点排序能力
   * 排序顺序: 正常节点 -> 慢节点 -> stale节点 -> 进入维护 -> 退役中 -> 已退役
   */
  @InterfaceAudience.Private 
  public static class StaleAndSlowComparator extends ServiceComparator {
    private final boolean avoidStaleDataNodesForRead;
    private final long staleInterval;
    private final boolean avoidSlowDataNodesForRead;
    private final Set<String> slowNodesUuidSet;

    /**
     * 构造函数
     * @param avoidStaleDataNodesForRead 是否避免读请求落到stale节点
     * @param interval 判定stale节点的时间间隔，可动态更新
     * @param avoidSlowDataNodesForRead 是否避免读请求落到慢节点
     * @param slowNodesUuidSet 慢节点的UUID集合
     */
    public StaleAndSlowComparator(
        boolean avoidStaleDataNodesForRead, long interval,
        boolean avoidSlowDataNodesForRead, Set<String> slowNodesUuidSet) {
      this.avoidStaleDataNodesForRead = avoidStaleDataNodesForRead;
      this.staleInterval = interval;
      this.avoidSlowDataNodesForRead = avoidSlowDataNodesForRead;
      this.slowNodesUuidSet = slowNodesUuidSet;
    }

    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      int ret = super.compare(a, b);
      if (ret != 0) {
        return ret;
      }

      // stale节点排到正常节点之后
      if (avoidStaleDataNodesForRead) {
        boolean aStale = a.isStale(staleInterval);
        boolean bStale = b.isStale(staleInterval);
        ret = aStale == bStale ? 0 : (aStale ? 1 : -1);
        if (ret != 0) {
          return ret;
        }
      }

      // 慢节点排到正常节点之后
      if (avoidSlowDataNodesForRead) {
        boolean aSlow = slowNodesUuidSet.contains(a.getDatanodeUuid());
        boolean bSlow = slowNodesUuidSet.contains(b.getDatanodeUuid());
        ret = aSlow == bSlow ? 0 : (aSlow ? 1 : -1);
      }
      return ret;
    }
  }    
    
  /**
   * 匹配目标地址是否为本地地址的匹配器
   */
  static final AddressMatcher LOCAL_ADDRESS_MATCHER = new AddressMatcher() {
    @Override
    public boolean match(InetSocketAddress s) {
      return NetUtils.isLocalAddress(s.getAddress());
    };
  };
  
  /**
   * 检查路径名是否合法
   */
  public static boolean isValidName(String src) {
    return DFSUtilClient.isValidName(src);
  }

  /**
   * 检查路径组件是否合法，加载FSImage时使用
   * 路径组件不能包含:或/，也不能是保留组件如.snapshot
   * @return 如果组件合法返回true
   */
  public static boolean isValidNameForComponent(String component) {
    if (component.equals(".") ||
        component.equals("..") ||
        component.indexOf(":") >= 0 ||
        component.indexOf("/") >= 0) {
      return false;
    }
    return !isReservedPathComponent(component);
  }


  /**
   * 检查路径组件是否为HDFS保留组件
   * @return true 如果组件是保留组件
   */
  public static boolean isReservedPathComponent(String component) {
    for (String reserved : HdfsServerConstants.RESERVED_PATH_COMPONENTS) {
      if (component.equals(reserved)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 将字节数组转换为UTF8编码字符串
   */
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }
  
  /**
   * 将字节数组指定范围解码为UTF8字符串
   * @param bytes 待解码字节数组
   * @param offset 起始偏移量
   * @param length 解码长度
   * @return 解码后的字符串
   */
  public static String bytes2String(byte[] bytes, int offset, int length) {
    return DFSUtilClient.bytes2String(bytes, 0, bytes.length);
  }

  /**
   * 将字符串转换为UTF8编码字节数组
   */
  public static byte[] string2Bytes(String str) {
    return DFSUtilClient.string2Bytes(str);
  }

  /**
   * 将路径组件数组合并为完整路径字符串（UTF8编码）
   */
  public static String byteArray2PathString(final byte[][] components,
      final int offset, final int length) {
    // 不使用StringBuilder减少不必要的字节拷贝和字符集转换
    final int range = offset + length;
    if (offset < 0 || range < offset || range > components.length) {
      throw new IndexOutOfBoundsException(
          "Incorrect index [offset, range, size] ["
              + offset + ", " + range + ", " + components.length + "]");
    }
    if (length == 0) {
      return "";
    }
    // 绝对路径开头为null或空字节数组
    byte[] firstComponent = components[offset];
    boolean isAbsolute = (offset == 0 &&
        (firstComponent == null || firstComponent.length == 0));
    if (offset == 0 && length == 1) {
      return isAbsolute ? Path.SEPARATOR : bytes2String(firstComponent);
    }
    // 预计算最终字节数组长度，包含分隔符
    int pos = isAbsolute ? 0 : firstComponent.length;
    int size = pos + length - 1;
    for (int i=offset + 1; i < range; i++) {
      size += components[i].length;
    }
    final byte[] result = new byte[size];
    if (!isAbsolute) {
      System.arraycopy(firstComponent, 0, result, 0, firstComponent.length);
    }
    // 依次追加每个组件，格式为 /component
    for (int i=offset + 1; i < range; i++) {
      result[pos++] = (byte)Path.SEPARATOR_CHAR;
      int len = components[i].length;
      System.arraycopy(components[i], 0, result, pos, len);
      pos += len;
    }
    return bytes2String(result);
  }

  public static String byteArray2PathString(byte[][] pathComponents) {
    return byteArray2PathString(pathComponents, 0, pathComponents.length);
  }

  /**
   * 将字符串路径组件使用路径分隔符合并为完整路径
   * @param components 路径组件数组
   * @return 合并后的UTF8路径字符串
   */
  public static String strings2PathString(String[] components) {
    if (components.length == 0) {
      return "";
    }
    if (components.length == 1) {
      if (components[0] == null || components[0].isEmpty()) {
        return Path.SEPARATOR;
      }
    }
    return Joiner.on(Path.SEPARATOR).join(components);
  }

  /** Convert an object representing a path to a string. */
  public static String path2String(final Object path) {
    return path == null? null
        : path instanceof String? (String)path
        : path instanceof byte[][]? byteArray2PathString((byte[][])path)
        : path.toString();
  }

  /**
   * 将路径字符串分割为字节数组形式的路径组件
   */
  public static byte[][] getPathComponents(String path) {
    // 避免中间分割为String数组，减少拷贝开销
    final byte[] bytes = string2Bytes(path);
    return DFSUtilClient
        .bytes2byteArray(bytes, bytes.length, (byte) Path.SEPARATOR_CHAR);
  }

  /**
   * 按指定分隔符将字节数组分割为多个字节数组
   * @param bytes 待分割字节数组
   * @param separator 分隔字节
   */
  public static byte[][] bytes2byteArray(byte[] bytes, byte separator) {
    return bytes2byteArray(bytes, bytes.length, separator);
  }

  /**
   * 按指定分隔符分割字节数组前len个字节
   * @param bytes 待分割字节数组
   * @param len 分割字节长度
   * @param separator 分隔字节
   */
  public static byte[][] bytes2byteArray(byte[] bytes, int len,
      byte separator) {
    return DFSUtilClient.bytes2byteArray(bytes, len, separator);
  }

  /**
   * 为基础配置key拼接多个后缀，生成完整配置key格式 key.suffix1.suffix2...suffixN
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = DFSUtilClient.concatSuffixes(suffixes);
    return DFSUtilClient.addSuffix(key, keySuffix);
  }

  /**
   * 获取指定名称服务下所有NameNode的RPC地址
   * @param conf 配置对象
   * @param nsId 名称服务ID
   * @param defaultValue 配置不存在时的默认地址
   * @return nnId -> RPC地址 的映射表
   */
  public static Map<String, InetSocketAddress> getRpcAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue) {
    return DFSUtilClient.getAddressesForNameserviceId(conf, nsId, defaultValue