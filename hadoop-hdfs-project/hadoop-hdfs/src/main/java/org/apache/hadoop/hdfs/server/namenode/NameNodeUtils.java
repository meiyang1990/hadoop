// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

/**
 * 文件级注释：NameNode节点工具类，提供HDFS NameNode相关配置解析、地址获取等公共工具能力
 * Utility functions for the NameNode.
 */
@InterfaceAudience.Private
public final class NameNodeUtils {
  public static final Logger LOG = LoggerFactory.getLogger(NameNodeUtils.class);

  /**
   * 获取客户端访问当前NameNode或名称服务的连接地址，必须在配置覆盖前调用获取原始配置
   * 根据不同部署模式（单节点、HA、联邦）返回对应地址：
   * <ol>
   * <li>未配置默认文件系统：返回null</li>
   * <li>默认文件系统无主机名：返回null</li>
   * <li>单节点NameNode（无HA、无联邦）：返回fs.defaultFS中的URI地址</li>
   * <li>当前NameNode属于HA名称服务：返回当前名称服务ID</li>
   * <li>联邦集群非HA场景：返回dfs.namenode.rpc-address配置的地址，配置不存在则回退使用fs.defaultFS地址</li>
   * <li>地址中无有效端口（端口缺失或为0）：返回null</li>
   * </ol>
   * @param conf Hadoop配置对象
   * @param nsId 当前NameNode所属的名称服务ID，可为null
   * @return 客户端连接地址，无法确定时返回null
   */
  @VisibleForTesting
  @Nullable
  static String getClientNamenodeAddress(
      Configuration conf, @Nullable String nsId) {
    // 从配置中获取所有名称服务ID列表
    final Collection<String> nameservices =
        DFSUtilClient.getNameServiceIds(conf);

    // 获取默认文件系统配置地址
    final String nnAddr = conf.getTrimmed(FS_DEFAULT_NAME_KEY);
    if (nnAddr == null) {
      // default fs is not set.
      return null;
    }

    LOG.info("{} is {}", FS_DEFAULT_NAME_KEY, nnAddr);
    // 解析默认文件系统地址为URI
    final URI nnUri = URI.create(nnAddr);

    // 从URI中提取主机名
    String defaultNnHost = nnUri.getHost();
    if (defaultNnHost == null) {
      return null;
    }

    // Current Nameservice is HA.
    // 当前名称服务存在且属于HA模式，检查是否配置了多个NameNode
    if (nsId != null && nameservices.contains(nsId)) {
      final Collection<String> namenodes = conf.getTrimmedStringCollection(
          DFS_HA_NAMENODES_KEY_PREFIX + "." + nsId);
      // HA模式下配置多个NameNode，直接返回名称服务ID供客户端解析
      if (namenodes.size() > 1) {
        return nsId;
      }
    }

    // Federation without HA. We must handle the case when the current NN
    // is not in the default nameservice.
    // 非HA联邦场景，尝试获取当前名称服务的RPC地址配置
    String currentNnAddress = null;
    if (nsId != null) {
      String hostNameKey = DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nsId;
      currentNnAddress = conf.get(hostNameKey);
    }

    // Fallback to the address in fs.defaultFS.
    // 当前名称服务无单独配置，回退使用默认文件系统的地址
    if (currentNnAddress == null) {
      currentNnAddress = nnUri.getAuthority();
    }

    // 解析地址中的端口号
    int port = 0;
    if (currentNnAddress.contains(":")) {
      port = Integer.parseInt(currentNnAddress.split(":")[1]);
    }

    // 端口有效则返回地址，否则返回null
    if (port > 0) {
       return currentNnAddress;
    } else {
      // the port is missing or 0. Figure out real bind address later.
      return null;
    }
  }

  /**
   * 工具类私有构造方法，禁止实例化
   */
  private NameNodeUtils() {
    // Disallow construction
  }
}