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

package org.apache.hadoop.hdfs.security.token.block;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * 用于传输数据块密钥信息的可序列化对象，在HDFS数据节点和NameNode之间传递块令牌加密密钥
 */
@InterfaceAudience.Private
public class ExportedBlockKeys implements Writable {
  /** 空密钥占位对象，用于未开启块令牌认证的场景 */
  public static final ExportedBlockKeys DUMMY_KEYS = new ExportedBlockKeys();
  /** 是否开启块令牌认证功能 */
  private boolean isBlockTokenEnabled;
  /** 密钥更新间隔，单位毫秒 */
  private long keyUpdateInterval;
  /** 块令牌有效期，单位毫秒 */
  private long tokenLifetime;
  /** 当前使用的最新签名密钥 */
  private final BlockKey currentKey;
  /** 所有有效的密钥列表，用于验证不同时间生成的块令牌 */
  private BlockKey[] allKeys;

  /**
   * 构造空的ExportedBlockKeys对象，供Hadoop序列化机制使用
   */
  public ExportedBlockKeys() {
    this(false, 0, 0, new BlockKey(), new BlockKey[0]);
  }

  /**
   * 构造包含完整密钥信息的ExportedBlockKeys对象
   * @param isBlockTokenEnabled 是否开启块令牌认证
   * @param keyUpdateInterval 密钥更新间隔（毫秒）
   * @param tokenLifetime 块令牌有效期（毫秒）
   * @param currentKey 当前活跃的签名密钥
   * @param allKeys 所有有效的密钥数组
   */
  public ExportedBlockKeys(boolean isBlockTokenEnabled, long keyUpdateInterval,
      long tokenLifetime, BlockKey currentKey, BlockKey[] allKeys) {
    this.isBlockTokenEnabled = isBlockTokenEnabled;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.currentKey = currentKey == null ? new BlockKey() : currentKey;
    this.allKeys = allKeys == null ? new BlockKey[0] : allKeys;
  }

  /**
   * 获取块令牌认证是否开启的状态
   * @return true表示已开启，false表示未开启
   */
  public boolean isBlockTokenEnabled() {
    return isBlockTokenEnabled;
  }

  /**
   * 获取密钥更新间隔
   * @return 密钥更新间隔，单位毫秒
   */
  public long getKeyUpdateInterval() {
    return keyUpdateInterval;
  }

  /**
   * 获取块令牌有效期
   * @return 块令牌有效期，单位毫秒
   */
  public long getTokenLifetime() {
    return tokenLifetime;
  }

  /**
   * 获取当前活跃的签名密钥
   * @return 当前用于生成新块令牌的密钥
   */
  public BlockKey getCurrentKey() {
    return currentKey;
  }

  /**
   * 获取所有有效的密钥列表
   * @return 所有可用于验证块令牌的密钥数组
   */
  public BlockKey[] getAllKeys() {
    return allKeys;
  }
  
  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////
  // 注册当前类的Writable工厂，供Hadoop反序列化动态创建实例
  static { // register a ctor
    WritableFactories.setFactory(ExportedBlockKeys.class,
        new WritableFactory() {
          @Override
          public Writable newInstance() {
            return new ExportedBlockKeys();
          }
        });
  }

  /**
   * 将对象序列化输出到DataOutput流
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // 写入块令牌启用状态
    out.writeBoolean(isBlockTokenEnabled);
    // 写入密钥更新间隔
    out.writeLong(keyUpdateInterval);
    // 写入块令牌有效期
    out.writeLong(tokenLifetime);
    // 写入当前密钥
    currentKey.write(out);
    // 写入密钥数组长度
    out.writeInt(allKeys.length);
    // 依次写入所有密钥
    for (int i = 0; i < allKeys.length; i++) {
      allKeys[i].write(out);
    }
  }

  /**
   * 从DataInput流反序列化读取对象字段
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取块令牌启用状态
    isBlockTokenEnabled = in.readBoolean();
    // 读取密钥更新间隔
    keyUpdateInterval = in.readLong();
    // 读取块令牌有效期
    tokenLifetime = in.readLong();
    // 读取当前密钥
    currentKey.readFields(in);
    // 读取密钥数组长度并初始化数组
    this.allKeys = new BlockKey[in.readInt()];
    // 依次反序列化所有密钥
    for (int i = 0; i < allKeys.length; i++) {
      allKeys[i] = new BlockKey();
      allKeys[i].readFields(in);
    }
  }

}