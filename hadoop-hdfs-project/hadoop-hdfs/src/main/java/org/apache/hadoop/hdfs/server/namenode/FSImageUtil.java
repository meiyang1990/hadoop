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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.Loader;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * FSImage镜像文件工具类，提供FSImage文件格式校验、摘要加载、压缩流包装等通用能力
 * 为NameNode加载FSImage提供基础工具支持
 */
@InterfaceAudience.Private
public final class FSImageUtil {
  public static final byte[] MAGIC_HEADER =
      "HDFSIMG1".getBytes(StandardCharsets.UTF_8);
  public static final int FILE_VERSION = 1;

  /**
   * 检查给定文件是否为合法的Protobuf格式FSImage镜像文件
   * @param file 待检查的FSImage随机访问文件
   * @return 是否为合法FSImage文件
   * @throws IOException IO读取失败时抛出异常
   */
  public static boolean checkFileFormat(RandomAccessFile file)
      throws IOException {
    // 文件长度小于最小要求，直接判定为不合法
    if (file.length() < Loader.MINIMUM_FILE_LENGTH)
      return false;

    byte[] magic = new byte[MAGIC_HEADER.length];
    // 读取文件开头的魔术头
    file.readFully(magic);
    // 比对魔术头是否匹配，不匹配则返回false
    if (!Arrays.equals(MAGIC_HEADER, magic))
      return false;

    return true;
  }

  /**
   * 从FSImage文件末尾加载文件摘要信息
   * @param file 待读取的FSImage随机访问文件
   * @return 解析后的FSImage文件摘要对象
   * @throws IOException 读取失败、版本不支持或格式非法时抛出异常
   */
  public static FileSummary loadSummary(RandomAccessFile file)
      throws IOException {
    final int FILE_LENGTH_FIELD_SIZE = 4;
    long fileLength = file.length();
    // 定位到文件末尾：摘要长度字段的起始位置
    file.seek(fileLength - FILE_LENGTH_FIELD_SIZE);
    // 读取摘要内容的长度
    int summaryLength = file.readInt();

    if (summaryLength <= 0) {
      throw new IOException("Negative length of the file");
    }
    // 定位到摘要内容的起始位置
    file.seek(fileLength - FILE_LENGTH_FIELD_SIZE - summaryLength);

    byte[] summaryBytes = new byte[summaryLength];
    // 读取完整的摘要字节内容
    file.readFully(summaryBytes);

    // 从字节流解析Protobuf格式的文件摘要
    FileSummary summary = FileSummary
        .parseDelimitedFrom(new ByteArrayInputStream(summaryBytes));
    // 校验文件版本是否支持
    if (summary.getOndiskVersion() != FILE_VERSION) {
      throw new IOException("Unsupported file version "
          + summary.getOndiskVersion());
    }

    // 校验布局版本是否支持当前Protobuf格式
    if (!NameNodeLayoutVersion.supports(Feature.PROTOBUF_FORMAT,
        summary.getLayoutVersion())) {
      throw new IOException("Unsupported layout version "
          + summary.getLayoutVersion());
    }
    return summary;
  }

  /**
   * 根据压缩编解码器配置包装输入流，创建对应压缩解压流
   * @param conf Hadoop配置对象
   * @param codec 压缩编Codec名称
   * @param in 原始输入流
   * @return 包装后的压缩解压输入流，如果未指定压缩则返回原始流
   * @throws IOException 创建压缩流失败时抛出异常
   */
  public static InputStream wrapInputStreamForCompression(
      Configuration conf, String codec, InputStream in) throws IOException {
    if (codec.isEmpty())
      return in;

    FSImageCompression compression = FSImageCompression.createCompression(
        conf, codec);
    CompressionCodec imageCodec = compression.getImageCodec();
    return imageCodec.createInputStream(in);
  }

}