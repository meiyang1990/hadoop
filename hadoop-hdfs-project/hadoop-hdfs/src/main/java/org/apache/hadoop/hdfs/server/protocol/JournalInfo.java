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

/**
 * HDFS日志（Journal）元信息描述类，用于在HA架构的NameNode节点间传递日志管理器基本信息
 * 包含日志的版本、集群标识和命名空间标识等核心元数据
 */
@InterfaceAudience.Private
public class JournalInfo {
  private final int layoutVersion;
  private final String clusterId;
  private final int namespaceId;

  /**
   * 构造Journal信息对象，存储日志管理器核心元数据
   * @param lv 日志布局版本号
   * @param clusterId HDFS集群唯一标识
   * @param nsId 命名空间唯一标识
   */
  public JournalInfo(int lv, String clusterId, int nsId) {
    this.layoutVersion = lv;
    this.clusterId = clusterId;
    this.namespaceId = nsId;
  }

  /**
   * 获取日志布局版本号
   * @return 布局版本号
   */
  public int getLayoutVersion() {
    return layoutVersion;
  }

  /**
   * 获取HDFS集群唯一标识
   * @return 集群ID
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * 获取命名空间唯一标识
   * @return 命名空间ID
   */
  public int getNamespaceId() {
    return namespaceId;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("lv=").append(layoutVersion).append(";cid=").append(clusterId)
    .append(";nsid=").append(namespaceId);
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    JournalInfo jInfo;
    if (!(o instanceof JournalInfo)) {
      return false;
    }
    jInfo = (JournalInfo) o;
    return ((jInfo.clusterId.equals(this.clusterId))
        && (jInfo.namespaceId == this.namespaceId)
        && (jInfo.layoutVersion == this.layoutVersion));
  }
  
  @Override
  public int hashCode() {
    return (namespaceId ^ layoutVersion ^ clusterId.hashCode());
  }
}