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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.CombinedIPList;

/**
 * HDFS数据传输可信通道解析器实现，基于黑名单判断IP/主机/子网是否可信
 * 维护服务端和客户端两份黑名单，用于过滤不可信的数据传输连接
 */
public class BlackListBasedTrustedChannelResolver extends
    TrustedChannelResolver {

  // 服务端侧黑名单，识别不可信客户端地址
  private CombinedIPList blackListForServer;
  // 客户端侧黑名单，识别不可信服务端地址
  private CombinedIPList blackListForClient;

  // 固定黑名单默认文件路径
  private static final String FIXED_BLACK_LIST_DEFAULT_LOCATION = "/etc/hadoop"
      + "/fixedBlackList";

  // 动态黑名单默认文件路径
  private static final String VARIABLE_BLACK_LIST_DEFAULT_LOCATION = "/etc/"
      + "hadoop/blackList";

  /**
   * 服务端配置：固定黑名单文件路径，包含需要拉黑的子网和IP地址
   */
  public static final String DFS_DATATRANSFER_SERVER_FIXED_BLACK_LIST_FILE =
      "dfs.datatransfer.server.fixedBlackList.file";
  /**
   * 服务端配置：是否启用动态黑名单
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_ENABLE
      = "dfs.datatransfer.server.variableBlackList.enable";
  /**
   * 服务端配置：动态黑名单文件路径，包含需要拉黑的子网和IP地址
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_FILE =
      "dfs.datatransfer.server.variableBlackList.file";
  /**
   * 服务端配置：动态黑名单文件缓存过期时间，单位秒，过期后重新加载文件
   */
  public static final String
      DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_CACHE_SECS = "dfs."
      + "datatransfer.server.variableBlackList.cache.secs";

  /**
   * 客户端配置：固定黑名单文件路径，包含需要拉黑的子网和IP地址
   */
  public static final String DFS_DATATRANSFER_CLIENT_FIXED_BLACK_LIST_FILE =
      "dfs.datatransfer.client.fixedBlackList.file";
  /**
   * 客户端配置：是否启用动态黑名单
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_ENABLE
      = "dfs.datatransfer.client.variableBlackList.enable";
  /**
   * 客户端配置：动态黑名单文件路径，包含需要拉黑的子网和IP地址
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_FILE =
      "dfs.datatransfer.client.variableBlackList.file";
  /**
   * 客户端配置：动态黑名单文件缓存过期时间，单位秒，过期后重新加载文件
   */
  public static final String
      DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_CACHE_SECS =
      "dfs.datatransfer.client.variableBlackList.cache.secs";

  /**
   * 加载配置并初始化服务端和客户端的黑名单
   * @param conf Hadoop配置对象
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    // 读取服务端固定黑名单文件路径
    String fixedFile = conf.get(DFS_DATATRANSFER_SERVER_FIXED_BLACK_LIST_FILE,
        FIXED_BLACK_LIST_DEFAULT_LOCATION);
    String variableFile = null;
    long expiryTime = 0;

    // 如果启用服务端动态黑名单，读取动态黑名单配置
    if (conf
        .getBoolean(DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_ENABLE,
            false)) {
      variableFile = conf.get(DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_FILE,
          VARIABLE_BLACK_LIST_DEFAULT_LOCATION);
      expiryTime =
          conf.getLong(DFS_DATATRANSFER_SERVER_VARIABLE_BLACK_LIST_CACHE_SECS,
              3600) * 1000;
    }

    // 创建服务端合黑名单（合并固定+动态）
    blackListForServer = new CombinedIPList(fixedFile, variableFile,
        expiryTime);

    // 读取客户端固定黑名单文件路径
    fixedFile = conf
        .get(DFS_DATATRANSFER_CLIENT_FIXED_BLACK_LIST_FILE, fixedFile);
    expiryTime = 0;

    // 如果启用客户端动态黑名单，读取动态黑名单配置
    if (conf
        .getBoolean(DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_ENABLE,
            false)) {
      variableFile = conf
          .get(DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_FILE, variableFile);
      expiryTime =
          conf.getLong(DFS_DATATRANSFER_CLIENT_VARIABLE_BLACK_LIST_CACHE_SECS,
              3600) * 1000;
    }

    // 创建客户端合并黑名单（合并固定+动态）
    blackListForClient = new CombinedIPList(fixedFile, variableFile,
        expiryTime);
  }

  /**
   * 客户端侧判断当前节点自身是否在服务端黑名单中，判断是否可以建立可信出站连接
   * @return true表示当前地址可信，允许建立连接；false表示不可信
   */
  public boolean isTrusted() {
    try {
      return !blackListForClient
          .isIn(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      return true;
    }
  }

  /**
   * 服务端侧判断接入的客户端地址是否可信，判断是否允许建立入站连接
   * @param clientAddress 客户端地址
   * @return true表示客户端地址可信，允许连接；false表示不可信
   */
  public boolean isTrusted(InetAddress clientAddress) {
    return !blackListForServer.isIn(clientAddress.getHostAddress());
  }
}