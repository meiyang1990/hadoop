// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.security.token.delegation;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;

/**
 * YARN Router 代理令牌信息序列化工具类，利用包访问权限绕过封装实现DelegationTokenInformation的序列化。
 * 本类是临时解决方案，未来Hadoop版本应将此能力内置到DelegationTokenInformation类中。
 * 核心功能为提供代理令牌信息的编解码，支持Router跨RM转发令牌请求。
 */
public final class RouterDelegationTokenSupport {

  private RouterDelegationTokenSupport() {
  }

  /**
   * 将DelegationTokenInformation对象编码为Base64 URL安全字符串。
   * @param token 待编码的代理令牌信息对象
   * @return 编码后的Base64字符串
   */
  public static String encodeDelegationTokenInformation(DelegationTokenInformation token) {
    try {
      // 创建字节输出流缓存序列化结果
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bos);
      // 写入密码长度
      WritableUtils.writeVInt(out, token.password.length);
      // 写入密码字节数组
      out.write(token.password);
      // 写入续期截止时间
      out.writeLong(token.renewDate);
      // 刷新缓冲区
      out.flush();
      byte[] tokenInfoBytes = bos.toByteArray();
      // 使用URL安全Base64编码返回结果
      return Base64.getUrlEncoder().encodeToString(tokenInfoBytes);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to encode token.", ex);
    }
  }

  /**
   * 从字节数组解码还原DelegationTokenInformation对象。
   * @param tokenBytes 待解码的字节数组（Base64解码后的原始数据）
   * @return 还原后的DelegationTokenInformation对象
   * @throws IOException 解码过程中IO异常
   */
  public static DelegationTokenInformation decodeDelegationTokenInformation(byte[] tokenBytes)
      throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(tokenBytes));
    // 创建空对象，后续填充字段（利用包访问权限修改私有字段）
    DelegationTokenInformation token = new DelegationTokenInformation(0, null);
    // 读取密码长度
    int len = WritableUtils.readVInt(in);
    // 分配字节数组存储密码
    token.password = new byte[len];
    // 完整读取密码字节
    in.readFully(token.password);
    // 读取续期时间
    token.renewDate = in.readLong();
    return token;
  }
}