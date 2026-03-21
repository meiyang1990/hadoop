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

package org.apache.hadoop.yarn.server.security;

import java.nio.ByteBuffer;

import javax.crypto.SecretKey;

import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN服务端主密钥数据封装类，同时保存PB序列化记录和已生成的SecretKey对象，
 * 避免重复编解码密钥字节，提升加密操作性能。
 */
public class MasterKeyData {

  // PB序列化格式的主密钥记录，用于RPC传输
  private final MasterKey masterKeyRecord;
  // 缓存已生成的SecretKey对象，避免重复从字节解码，提升性能
  private final SecretKey generatedSecretKey;

  /**
   * 根据密钥序列号和密钥对象构造主密钥数据。
   * @param serialNo 主密钥序列号
   * @param secretKey JCE SecretKey对象
   */
  public MasterKeyData(int serialNo, SecretKey secretKey) {
    this.masterKeyRecord = Records.newRecord(MasterKey.class);
    this.masterKeyRecord.setKeyId(serialNo);
    this.generatedSecretKey = secretKey;
    this.masterKeyRecord.setBytes(ByteBuffer.wrap(generatedSecretKey
      .getEncoded()));
  }

  /**
   * 根据已有主密钥记录和密钥对象构造主密钥数据。
   * @param masterKeyRecord PB格式主密钥记录
   * @param secretKey JCE SecretKey对象
   */
  public MasterKeyData(MasterKey masterKeyRecord, SecretKey secretKey) {
    this.masterKeyRecord = masterKeyRecord;
    this.generatedSecretKey = secretKey;

  }

  /**
   * 获取PB格式的主密钥记录，用于RPC传输。
   * @return PB格式主密钥
   */
  public MasterKey getMasterKey() {
    return this.masterKeyRecord;
  }

  /**
   * 获取缓存的JCE SecretKey对象，用于直接进行加密操作。
   * @return 已生成的SecretKey
   */
  public SecretKey getSecretKey() {
    return this.generatedSecretKey;
  }
}