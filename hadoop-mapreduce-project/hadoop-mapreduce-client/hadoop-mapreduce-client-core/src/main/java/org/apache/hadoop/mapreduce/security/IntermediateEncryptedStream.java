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
package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.CryptoUtils;

/**
 * MapReduce中间溢出文件加密处理工具类，负责对溢出文件进行加密包装和回调注入
 * 通过注入{@link SpillCallBackInjector}实现两个核心功能：
 *   1. 捕获溢出文件的路径信息并回调
 *   2. 当开启中间加密功能时，验证溢出文件的加密正确性
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class IntermediateEncryptedStream {

  /** 保存之前设置的溢出回调注入器，用于重置恢复 */
  private static SpillCallBackInjector prevSpillCBInjector = null;

  /**
   * 若配置开启加密，则对输出流进行加密包装，并注入溢出文件写回调
   * @param conf Hadoop配置对象
   * @param out 原始输出流
   * @param outPath 溢出文件路径
   * @return 加密后的输出流（若未开启加密则返回原流）
   * @throws IOException 包装或回调处理失败时抛出
   */
  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out, Path outPath) throws IOException {
    SpillCallBackInjector.get().writeSpillFileCB(outPath, out, conf);
    return CryptoUtils.wrapIfNecessary(conf, out, true);
  }

  /**
   * 若配置开启加密，则对输出流进行加密包装，并注入溢出文件写回调
   * @param conf Hadoop配置对象
   * @param out 原始输出流
   * @param closeOutputStream 是否在关闭时关闭底层流
   * @param outPath 溢出文件路径
   * @return 加密后的输出流（若未开启加密则返回原流）
   * @throws IOException 包装或回调处理失败时抛出
   */
  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out, boolean closeOutputStream,
      Path outPath) throws IOException {
    SpillCallBackInjector.get().writeSpillFileCB(outPath, out, conf);
    return CryptoUtils.wrapIfNecessary(conf, out, closeOutputStream);
  }

  /**
   * 若配置开启加密，则对输入流进行加密包装，并注入溢出文件读回调
   * @param conf Hadoop配置对象
   * @param in 原始输入流
   * @param inputPath 溢出文件路径
   * @return 解密后的输入流（若未开启加密则返回原流）
   * @throws IOException 包装或回调处理失败时抛出
   */
  public static FSDataInputStream wrapIfNecessary(Configuration conf,
      FSDataInputStream in, Path inputPath) throws IOException {
    SpillCallBackInjector.get().getSpillFileCB(inputPath, in, conf);
    return CryptoUtils.wrapIfNecessary(conf, in);
  }

  /**
   * 若配置开启加密，则对通用输入流进行加密包装，并注入溢出文件读回调
   * @param conf Hadoop配置对象
   * @param in 原始输入流
   * @param length 流数据长度
   * @param inputPath 溢出文件路径
   * @return 解密后的输入流（若未开启加密则返回原流）
   * @throws IOException 包装或回调处理失败时抛出
   */
  public static InputStream wrapIfNecessary(Configuration conf,
      InputStream in, long length, Path inputPath) throws IOException {
    SpillCallBackInjector.get().getSpillFileCB(inputPath, in, conf);
    return CryptoUtils.wrapIfNecessary(conf, in, length);
  }

  /**
   * 回调注入溢出索引文件路径
   * @param indexFilename 溢出索引文件路径
   * @param conf Hadoop配置对象
   */
  public static void addSpillIndexFile(Path indexFilename, Configuration conf) {
    SpillCallBackInjector.get().addSpillIndexFileCB(indexFilename, conf);
  }

  /**
   * 验证溢出索引文件加密正确性
   * @param indexFilename 待验证的溢出索引文件路径
   * @param conf Hadoop配置对象
   */
  public static void validateSpillIndexFile(Path indexFilename,
      Configuration conf) {
    SpillCallBackInjector.get().validateSpillIndexFileCB(indexFilename, conf);
  }

  /**
   * 重置溢出回调注入器为之前保存的实例
   * @return 重置后的注入器实例
   */
  public static SpillCallBackInjector resetSpillCBInjector() {
    return setSpillCBInjector(prevSpillCBInjector);
  }

  /**
   * 设置当前溢出回调注入器，同时保存之前的实例用于后续重置
   * @param spillInjector 要设置的新注入器实例
   * @return 设置后的注入器实例
   */
  public synchronized static SpillCallBackInjector setSpillCBInjector(
      SpillCallBackInjector spillInjector) {
    prevSpillCBInjector =
        SpillCallBackInjector.getAndSet(spillInjector);
    return spillInjector;
  }

  /** 工具类禁止实例化 */
  private IntermediateEncryptedStream() {}
}