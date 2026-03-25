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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * 文件级注释：HDFS NameNode FSImage镜像文件压缩支持容器类
 * 负责管理FSImage压缩使用的编解码器，提供压缩头读写、输入输出流封装能力
 * 支持无压缩和配置指定编解码器两种模式，实现FSImage文件的压缩存储与读取
 *
 * Simple container class that handles support for compressed fsimage files.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageCompression {

  /** Codec to use to save or load image, or null if the image is not compressed */
  private CompressionCodec imageCodec;

  /**
   * 创建无压缩（NOOP）实例
   * Create a "noop" compression - i.e. uncompressed
   */
  private FSImageCompression() {
  }

  /**
   * 使用指定编解码器创建压缩实例
   * Create compression using a particular codec
   */
  private FSImageCompression(CompressionCodec codec) {
    imageCodec = codec;
  }

  /**
   * 获取当前使用的压缩编解码器
   * @return 压缩编解码器实例，无压缩时返回null
   */
  public CompressionCodec getImageCodec() {
    return imageCodec;
  }

  /**
   * 创建无压缩（不启用压缩）的FSImage压缩实例
   * @return 无压缩FSImage压缩实例
   */
  static FSImageCompression createNoopCompression() {
    return new FSImageCompression();
  }

  /**
   * 根据Hadoop配置创建FSImage压缩实例，从配置中读取是否压缩和编解码器配置
   * @param conf Hadoop配置对象
   * @return 压缩实例
   * @throws IOException 指定编解码器不可用时抛出异常
   */
  static FSImageCompression createCompression(Configuration conf)
    throws IOException {
    boolean compressImage = conf.getBoolean(
      DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY,
      DFSConfigKeys.DFS_IMAGE_COMPRESS_DEFAULT);

    if (!compressImage) {
      return createNoopCompression();
    }

    String codecClassName = conf.get(
      DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
      DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_DEFAULT);
    return createCompression(conf, codecClassName);
  }

  /**
   * 使用指定编解码器类名创建压缩实例
   * @param conf Hadoop配置对象
   * @param codecClassName 编解码器全类名
   * @return 压缩实例
   * @throws IOException 指定编解码器不可用时抛出异常
   */
  static FSImageCompression createCompression(Configuration conf,
                                                      String codecClassName)
    throws IOException {

    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodecByClassName(codecClassName);
    if (codec == null) {
      throw new IOException("Not a supported codec: " + codecClassName);
    }

    return new FSImageCompression(codec);
  }

  /**
   * 从输入流读取压缩头信息，创建对应压缩实例
   * @param conf Hadoop配置对象
   * @param in 输入流，用于读取压缩头
   * @return 对应压缩实例
   * @throws IOException IO错误或编解码器不可用时抛出异常
   */
  static FSImageCompression readCompressionHeader(
    Configuration conf, DataInput in) throws IOException
  {
    boolean isCompressed = in.readBoolean();

    if (!isCompressed) {
      return createNoopCompression();
    } else {
      String codecClassName = Text.readString(in);
      return createCompression(conf, codecClassName);
    }
  }
  
  /**
   * 对输入流解压包装，返回可读取解压后数据的输入流
   * 无压缩时仅添加缓冲处理
   * @param is 原始输入流
   * @return 包装后可读取解压数据的数据流
   * @throws IOException 解压实例创建失败或IO错误时抛出异常
   */
  DataInputStream unwrapInputStream(InputStream is) throws IOException {
    if (imageCodec != null) {
      return new DataInputStream(imageCodec.createInputStream(is));
    } else {
      return new DataInputStream(new BufferedInputStream(is));
    }
  }

  /**
   * 写入压缩头信息，并对输出流进行压缩包装，返回压缩后的输出流
   * 无压缩时仅添加缓冲处理，保证返回流始终带缓冲
   * @param os 原始输出流，要求为无缓冲
   * @return 包装后的压缩输出流（无压缩时为带缓冲输出流）
   * @throws IOException IO错误或压缩实例创建失败时抛出异常
   */
  DataOutputStream writeHeaderAndWrapStream(OutputStream os)
  throws IOException {
    DataOutputStream dos = new DataOutputStream(os);

    // 写入是否启用压缩的标志位
    dos.writeBoolean(imageCodec != null);

    if (imageCodec != null) {
      // 写入编解码器类名供读取时恢复
      String codecClassName = imageCodec.getClass().getCanonicalName();
      Text.writeString(dos, codecClassName);

      return new DataOutputStream(imageCodec.createOutputStream(os));
    } else {
      // use a buffered output stream
      return new DataOutputStream(new BufferedOutputStream(os));
    }
  }

  @Override
  public String toString() {
    if (imageCodec != null) {
      return "codec " + imageCodec.getClass().getCanonicalName();
    } else {
      return "no compression";
    }
  }
}