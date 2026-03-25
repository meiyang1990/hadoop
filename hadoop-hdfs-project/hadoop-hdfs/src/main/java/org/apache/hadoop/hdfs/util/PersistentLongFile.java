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
package org.apache.hadoop.hdfs.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

/**
 * 持久化存储单个Long值的磁盘文件工具类
 * 保证文件更新是原子性的，并且会执行fsync确保数据持久化到磁盘
 * 用于HDFS中需要可靠持久化单个长整型数值的场景（例如事务ID、世代ID等）
 */
@InterfaceAudience.Private
public class PersistentLongFile {
  private static final Logger LOG = LoggerFactory.getLogger(
      PersistentLongFile.class);

  private final File file;
  private final long defaultVal;
  
  private long value;
  private boolean loaded = false;
  
  /**
   * 构造持久化Long文件对象
   * @param file 存储数值的目标文件
   * @param defaultVal 文件不存在或读取失败时使用的默认值
   */
  public PersistentLongFile(File file, long defaultVal) {
    this.file = file;
    this.defaultVal = defaultVal;
  }
  
  /**
   * 获取持久化存储的Long值，延迟加载模式，首次调用才会从磁盘读取
   * @return 存储的数值
   * @throws IOException 读取或解析文件失败时抛出
   */
  public long get() throws IOException {
    if (!loaded) {
      value = readFile(file, defaultVal);
      loaded = true;
    }
    return value;
  }
  
  /**
   * 设置并持久化新的数值，只有数值变化或未加载过才会写入磁盘
   * @param newVal 新的数值
   * @throws IOException 写入文件失败时抛出
   */
  public void set(long newVal) throws IOException {
    if (value != newVal || !loaded) {
      writeFile(file, newVal);
    }
    value = newVal;
    loaded = true;
  }

  /**
   * 原子性将数值写入目标文件，写入完成后会执行fsync确保数据落盘
   * @param file 目标文件
   * @param val 要写入的数值
   * @throws IOException 文件写入失败时抛出
   */
  public static void writeFile(File file, long val) throws IOException {
    // 使用原子化输出流保证写入原子性
    AtomicFileOutputStream fos = new AtomicFileOutputStream(file);
    try {
      // 将数值转为UTF-8字节写入
      fos.write(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
      // 写入换行符分隔
      fos.write('\n');
      // 关闭流完成原子替换，此时数据已fsync落盘
      fos.close();
      fos = null;
    } finally {
      // 异常发生时中断写入，清理临时文件
      if (fos != null) {
        fos.abort();        
      }
    }
  }

  /**
   * 从指定文件读取Long数值，文件不存在则返回默认值
   * @param file 要读取的文件
   * @param defaultVal 文件不存在时的默认返回值
   * @return 读取到的数值
   * @throws IOException 读取文件或数值格式错误时抛出
   */
  public static long readFile(File file, long defaultVal) throws IOException {
    long val = defaultVal;
    // 文件存在才读取，否则直接返回默认值
    if (file.exists()) {
      BufferedReader br = 
          new BufferedReader(new InputStreamReader(new FileInputStream(
              file), StandardCharsets.UTF_8));
      try {
        // 读取第一行并解析为Long
        val = Long.parseLong(br.readLine());
        br.close();
        br = null;
      } catch (NumberFormatException e) {
        // 数值格式错误，抛出IO异常
        throw new IOException(e);
      } finally {
        // 确保流被正确关闭，记录关闭异常
        IOUtils.cleanupWithLogger(LOG, br);
      }
    }
    return val;
  }
}