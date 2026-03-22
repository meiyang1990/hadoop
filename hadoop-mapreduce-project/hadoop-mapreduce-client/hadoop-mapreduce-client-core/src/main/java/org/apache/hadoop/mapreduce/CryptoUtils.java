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
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataInputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LimitInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：MapReduce中间数据加密工具类，为加密流提供封装工具，主要用于处理MapReduce spill溢写文件的加密/解密操作
 *
 * This class provides utilities to make it easier to work with Cryptographic
 * Streams. Specifically for dealing with encrypting intermediate data such
 * MapReduce spill files.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CryptoUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CryptoUtils.class);

  /**
   * 检查是否开启MapReduce中间数据溢写加密
   * @param conf Hadoop配置对象
   * @return true表示开启加密，false表示不开启
   */
  public static boolean isEncryptedSpillEnabled(Configuration conf) {
    return conf.getBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA);
  }

  /**
   * This method creates and initializes an IV (Initialization Vector)
   * 
   * @param conf configuration
   * @return byte[] initialization vector
   * @throws IOException exception in case of error
   */
  public static byte[] createIV(Configuration conf) throws IOException {
    CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
    if (isEncryptedSpillEnabled(conf)) {
      // 根据加密算法块大小初始化IV数组
      byte[] iv = new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
      // 生成安全随机数作为初始向量
      cryptoCodec.generateSecureRandom(iv);
      cryptoCodec.close();
      return iv;
    } else {
      return null;
    }
  }

  /**
   * 计算加密所需额外填充字节长度，包含IV长度和文件偏移量存储长度
   * @param conf Hadoop配置对象
   * @return 填充字节总长度
   * @throws IOException 创建CryptoCodec失败时抛出
   */
  public static int cryptoPadding(Configuration conf) throws IOException {
    // Sizeof(IV) + long(start-offset)
    if (!isEncryptedSpillEnabled(conf)) {
      return 0;
    }
    final CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
    try {
      // IV长度等于算法块大小，加上8字节存储起始偏移量
      return cryptoCodec.getCipherSuite().getAlgorithmBlockSize() + 8;
    } finally {
      cryptoCodec.close();
    }
  }

  /**
   * 从当前用户凭证中获取溢写加密密钥
   * @return 加密密钥字节数组
   * @throws IOException 获取当前用户或密钥失败时抛出
   */
  private static byte[] getEncryptionKey() throws IOException {
    return TokenCache.getEncryptedSpillKey(UserGroupInformation.getCurrentUser()
            .getCredentials());
  }

  /**
   * 从配置中读取加密缓冲区大小并转换为字节单位
   * @param conf Hadoop配置对象
   * @return 加密缓冲区大小（字节）
   */
  private static int getBufferSize(Configuration conf) {
    return conf.getInt(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB) * 1024;
  }

  /**
   * Wraps a given FSDataOutputStream with a CryptoOutputStream. The size of the
   * data buffer required for the stream is specified by the
   * "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
   * variable.
   * 
   * @param conf configuration
   * @param out given output stream
   * @return FSDataOutputStream encrypted output stream if encryption is
   *         enabled; otherwise the given output stream itself
   * @throws IOException exception in case of error
   */
  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out) throws IOException {
    return wrapIfNecessary(conf, out, true);
  }

  /**
   * Wraps a given FSDataOutputStream with a CryptoOutputStream. The size of the
   * data buffer required for the stream is specified by the
   * "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
   * variable.
   *
   * @param conf configuration
   * @param out given output stream
   * @param closeOutputStream flag to indicate whether closing the wrapped
   *        stream will close the given output stream
   * @return FSDataOutputStream encrypted output stream if encryption is
   *         enabled; otherwise the given output stream itself
   * @throws IOException exception in case of error
   */
  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out, boolean closeOutputStream) throws IOException {
    if (isEncryptedSpillEnabled(conf)) {
      // 写入当前流位置偏移量，用于后续解密定位
      out.write(ByteBuffer.allocate(8).putLong(out.getPos()).array());
      // 生成随机初始向量IV
      byte[] iv = createIV(conf);
      // 写入IV到文件头部
      out.write(iv);
      if (LOG.isDebugEnabled()) {
        LOG.debug("IV written to Stream ["
            + Base64.encodeBase64URLSafeString(iv) + "]");
      }
      // 返回加密封装后的输出流
      return new CryptoFSDataOutputStream(out, CryptoCodec.getInstance(conf),
          getBufferSize(conf), getEncryptionKey(), iv, closeOutputStream);
    } else {
      // 未开启加密则返回原流
      return out;
    }
  }

  /**
   * Wraps a given InputStream with a CryptoInputStream. The size of the data
   * buffer required for the stream is specified by the
   * "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
   * variable.
   * 
   * If the value of 'length' is &gt; -1, The InputStream is additionally
   * wrapped in a LimitInputStream. CryptoStreams are late buffering in nature.
   * This means they will always try to read ahead if they can. The
   * LimitInputStream will ensure that the CryptoStream does not read past the
   * provided length from the given Input Stream.
   * 
   * @param conf configuration
   * @param in given input stream
   * @param length maximum number of bytes to read from the input stream
   * @return InputStream encrypted input stream if encryption is
   *         enabled; otherwise the given input stream itself
   * @throws IOException exception in case of error
   */
  public static InputStream wrapIfNecessary(Configuration conf, InputStream in,
      long length) throws IOException {
    if (isEncryptedSpillEnabled(conf)) {
      int bufferSize = getBufferSize(conf);
      // 如果指定读取长度限制，使用LimitInputStream防止预读越界
      if (length > -1) {
        in = new LimitInputStream(in, length);
      }
      // 读取文件头部存储的偏移量信息
      byte[] offsetArray = new byte[8];
      IOUtils.readFully(in, offsetArray, 0, 8);
      long offset = ByteBuffer.wrap(offsetArray).getLong();
      CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
      // 读取文件头部存储的初始向量IV
      byte[] iv = 
          new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
      IOUtils.readFully(in, iv, 0, 
          cryptoCodec.getCipherSuite().getAlgorithmBlockSize());
      if (LOG.isDebugEnabled()) {
        LOG.debug("IV read from ["
            + Base64.encodeBase64URLSafeString(iv) + "]");
      }
      // 返回加密封装后的输入流，计算正确的起始偏移量
      return new CryptoInputStream(in, cryptoCodec, bufferSize,
          getEncryptionKey(), iv, offset + cryptoPadding(conf));
    } else {
      // 未开启加密则返回原流
      return in;
    }
  }

  /**
   * Wraps a given FSDataInputStream with a CryptoInputStream. The size of the
   * data buffer required for the stream is specified by the
   * "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
   * variable.
   * 
   * @param conf configuration
   * @param in given input stream
   * @return FSDataInputStream encrypted input stream if encryption is
   *         enabled; otherwise the given input stream itself
   * @throws IOException exception in case of error
   */
  public static FSDataInputStream wrapIfNecessary(Configuration conf,
      FSDataInputStream in) throws IOException {
    if (isEncryptedSpillEnabled(conf)) {
      CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
      int bufferSize = getBufferSize(conf);
      // 跳过文件头部存储的偏移量信息，FSDataInputStream自带位置管理，此处偏移量不使用但需要读出跳过
      // Since the O/P stream always writes it..
      IOUtils.readFully(in, new byte[8], 0, 8);
      // 读取文件头部存储的初始向量IV
      byte[] iv = 
          new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
      IOUtils.readFully(in, iv, 0, 
          cryptoCodec.getCipherSuite().getAlgorithmBlockSize());
      if (LOG.isDebugEnabled()) {
        LOG.debug("IV read from Stream ["
            + Base64.encodeBase64URLSafeString(iv) + "]");
      }
      // 返回加密封装后的输入流
      return new CryptoFSDataInputStream(in, cryptoCodec, bufferSize,
          getEncryptionKey(), iv);
    } else {
      // 未开启加密则返回原流
      return in;
    }
  }

}