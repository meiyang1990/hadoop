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
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;

/**
 * 文件级注释：HDFS节点间通信协议，定义名称节点向数据节点下发的密钥更新命令
 * 用于HDFS块访问令牌密钥的轮转更新，保障数据块访问认证安全性
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class KeyUpdateCommand extends DatanodeCommand {
  /** 更新后的块密钥集合 */
  private final ExportedBlockKeys keys;

  /**
   * 无参构造函数，构造空密钥更新命令（使用默认空密钥集合）
   */
  KeyUpdateCommand() {
    this(new ExportedBlockKeys());
  }

  /**
   * 构造携带指定新密钥的密钥更新命令
   * @param keys 更新后的导出块密钥集合
   */
  public KeyUpdateCommand(ExportedBlockKeys keys) {
    // 设置命令类型为密钥更新
    super(DatanodeProtocol.DNA_ACCESSKEYUPDATE);
    this.keys = keys;
  }

  /**
   * 获取名称节点下发的更新后的块密钥集合
   * @return 更新后的导出块密钥集合，供数据节点更新本地密钥缓存
   */
  public ExportedBlockKeys getExportedKeys() {
    return this.keys;
  }
}