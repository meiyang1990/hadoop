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
package org.apache.hadoop.mapreduce.security;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.crypto.SecretKey;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce Shuffle阶段安全工具类，提供密钥生成、哈希计算与验证功能，保障Shuffle数据传输安全
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SecureShuffleUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecureShuffleUtils.class);
  
  // HTTP请求头：URL哈希标识，用于验证请求完整性
  public static final String HTTP_HEADER_URL_HASH = "UrlHash";
  // HTTP响应头：响应哈希标识，用于验证响应完整性
  public static final String HTTP_HEADER_REPLY_URL_HASH = "ReplyHash";
  
  /**
   * 对消息生成Base64编码的哈希值
   * @param msg 待计算哈希的原始消息
   * @param key 用于计算HMAC的密钥
   * @return Base64编码后的哈希字符串
   */
  public static String generateHash(byte[] msg, SecretKey key) {
    return new String(Base64.encodeBase64(generateByteHash(msg, key)), 
        StandardCharsets.UTF_8);
  }
  
  /**
   * 计算消息的原始二进制哈希值
   * @param msg 待计算哈希的原始消息
   * @param key 用于计算HMAC的密钥
   * @return 二进制哈希结果
   */
  private static byte[] generateByteHash(byte[] msg, SecretKey key) {
    return JobTokenSecretManager.computeHash(msg, key);
  }
  
  /**
   * 验证给定哈希值与消息计算出的哈希是否一致
   * @param hash 待验证的哈希值
   * @param msg 原始消息
   * @param key 用于计算HMAC的密钥
   * @return true 哈希一致，false 哈希不一致
   */
  private static boolean verifyHash(byte[] hash, byte[] msg, SecretKey key) {
    byte[] msg_hash = generateByteHash(msg, key);
    return WritableComparator.compareBytes(msg_hash, 0, msg_hash.length, hash, 0, hash.length) == 0;
  }
  
  /**
   * 对输入字符串计算并生成Base64编码的哈希值
   * @param enc_str 待计算哈希的输入字符串
   * @param key 用于计算HMAC的密钥
   * @return Base64编码后的哈希字符串
   * @throws IOException 编码异常
   */
  public static String hashFromString(String enc_str, SecretKey key) 
  throws IOException {
    return generateHash(enc_str.getBytes(StandardCharsets.UTF_8), key);
  }
  
  /**
   * 验证Base64编码的哈希与消息计算结果是否一致，不一致则抛出异常
   * @param base64Hash Base64编码的待验证哈希
   * @param msg 原始消息
   * @param key 用于计算HMAC的密钥
   * @throws IOException 验证失败时抛出异常
   */
  public static void verifyReply(String base64Hash, String msg, SecretKey key)
  throws IOException {
    byte[] hash = Base64.decodeBase64(base64Hash.getBytes(StandardCharsets.UTF_8));
    
    boolean res = verifyHash(hash, msg.getBytes(StandardCharsets.UTF_8), key);
    
    if(res != true) {
      throw new IOException("Verification of the hashReply failed");
    }
  }
  
  /**
   * 从URL对象构造用于哈希计算的原始消息字符串
   * @param url 输入URL对象
   * @return 用于哈希计算的拼接消息字符串
   */
  public static String buildMsgFrom(URL url) {
    return buildMsgFrom(url.getPath(), url.getQuery(), url.getPort());
  }

  /**
   * 从HttpServletRequest对象构造用于哈希计算的原始消息字符串
   * @param request HTTP请求对象
   * @return 用于哈希计算的拼接消息字符串
   */
  public static String buildMsgFrom(HttpServletRequest request ) {
    return buildMsgFrom(request.getRequestURI(), request.getQueryString(),
        request.getLocalPort());
  }
  
  /**
   * 从URI路径、查询参数、端口拼接构造用于哈希计算的原始消息字符串
   * @param uri_path URI路径
   * @param uri_query URI查询参数
   * @param port 服务端口
   * @return 拼接完成的消息字符串
   */
  private static String buildMsgFrom(String uri_path, String uri_query, int port) {
    return String.valueOf(port) + uri_path + "?" + uri_query;
  }

  /**
   * 将字节数组转换为十六进制字符串
   * 
   * @param ba 输入字节数组
   * @return 字节数组对应的十六进制字符串
   */
  public static String toHex(byte[] ba) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String strHex = "";
    try {
      PrintStream ps = new PrintStream(baos, false, "UTF-8");
      for (byte b : ba) {
        ps.printf("%x", b);
      }
      strHex = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    } catch (UnsupportedEncodingException e) {
    }
    return strHex;
  }
}