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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.util.CombinedIPWhiteList;

/**
 * 基于IP白名单机制的可信通道解析器，用于在HDFS数据传输过程中判断对端节点是否为可信地址
 * 分别维护服务端侧和客户端侧两套白名单，支持固定白名单和可动态更新的可变白名单两种配置方式
 */
public class WhitelistBasedTrustedChannelResolver  extends TrustedChannelResolver {

  private CombinedIPWhiteList whiteListForServer;
  private CombinedIPWhiteList whitelistForClient;

  private static final String FIXEDWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/fixedwhitelist";

  private static final String VARIABLEWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/whitelist";

  /**
   * Path to the file to containing subnets and ip addresses to form fixed whitelist.
   */
  public static final String DFS_DATATRANSFER_SERVER_FIXEDWHITELIST_FILE =
    "dfs.datatransfer.server.fixedwhitelist.file";
  /**
   * Enables/Disables variable whitelist
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_ENABLE =
    "dfs.datatransfer.server.variablewhitelist.enable";
  /**
   * Path to the file to containing subnets and ip addresses to form variable whitelist.
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_FILE =
    "dfs.datatransfer.server.variablewhitelist.file";
  /**
   * time in seconds by which the variable whitelist file is checked for updates
   */
  public static final String DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_CACHE_SECS =
    "dfs.datatransfer.server.variablewhitelist.cache.secs";

  /**
   * Path to the file to containing subnets and ip addresses to form fixed whitelist.
   */
  public static final String DFS_DATATRANSFER_CLIENT_FIXEDWHITELIST_FILE =
    "dfs.datatransfer.client.fixedwhitelist.file";
  /**
   * Enables/Disables variable whitelist
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_ENABLE =
    "dfs.datatransfer.client.variablewhitelist.enable";
  /**
   * Path to the file to containing subnets and ip addresses to form variable whitelist.
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_FILE =
    "dfs.datatransfer.client.variablewhitelist.file";
  /**
   * time in seconds by which the variable whitelist file is checked for updates
   */
  public static final String DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_CACHE_SECS =
    "dfs.datatransfer.client.variablewhitelist.cache.secs";

  /**
   * 从配置中加载服务端和客户端侧的IP白名单，初始化白名单解析器
   * @param conf Hadoop配置对象
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    // 读取服务端固定白名单文件路径
    String fixedFile = conf.get(DFS_DATATRANSFER_SERVER_FIXEDWHITELIST_FILE,
        FIXEDWHITELIST_DEFAULT_LOCATION);
    String variableFile = null;
    long expiryTime = 0;

    // 如果启用服务端可变白名单，读取对应配置
    if (conf.getBoolean(DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_ENABLE, false)) {
      variableFile = conf.get(DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_FILE,
          VARIABLEWHITELIST_DEFAULT_LOCATION);
      expiryTime =
        conf.getLong(DFS_DATATRANSFER_SERVER_VARIABLEWHITELIST_CACHE_SECS,3600) * 1000;
    }

    // 初始化服务端侧组合IP白名单
    whiteListForServer = new CombinedIPWhiteList(fixedFile,variableFile,expiryTime);

    // 读取客户端固定白名单文件路径
    fixedFile = conf.get(DFS_DATATRANSFER_CLIENT_FIXEDWHITELIST_FILE, fixedFile);
    expiryTime = 0;

    // 如果启用客户端可变白名单，读取对应配置
    if (conf.getBoolean(DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_ENABLE, false)) {
      variableFile = conf.get(DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_FILE,variableFile);
      expiryTime =
        conf.getLong(DFS_DATATRANSFER_CLIENT_VARIABLEWHITELIST_CACHE_SECS,3600) * 1000;
    }

    // 初始化客户端侧组合IP白名单
    whitelistForClient = new CombinedIPWhiteList(fixedFile,variableFile,expiryTime);
  }

  /**
   * 客户端侧判断当前节点本地地址是否在白名单中，判断连接是否可信
   * @return 当前本地地址是否可信
   */
  public boolean isTrusted() {
    try {
      return whitelistForClient.isIn(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      return false;
    }
  }

  /**
   * 服务端侧判断指定客户端地址是否在白名单中，判断连接是否可信
   * @param clientAddress 客户端地址
   * @return 客户端地址是否可信
   */
  public boolean isTrusted(InetAddress clientAddress) {
    return whiteListForServer.isIn(clientAddress.getHostAddress());
  }
}