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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.thirdparty.com.google.common.io.Files;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Longs;

/**
 * 存储单个long类型数值的磁盘文件实现，不保证数据持久化 durability。
 * 和{@link PersistentLongFile}不同，本类不会在每次修改后强制刷盘同步。
 * 
 * 适用于更新频繁、性能要求高，且不需要强一致性保证的场景，正确性不依赖最新值持久化。
 * 相比PersistentLongFile，本类以二进制格式存储数值，而非文本字符串。
 */
@InterfaceAudience.Private
public class BestEffortLongFile implements Closeable {

  private final File file;
  private final long defaultVal;

  private long value;
  
  private FileChannel ch = null;
  
  private final ByteBuffer buf = ByteBuffer.allocate(Long.SIZE/8);

  /**
   * 构造BestEffortLongFile实例，指定文件路径和默认值。
   * @param file 存储数值的磁盘文件
   * @param defaultVal 文件不存在时使用的默认值
   */
  public BestEffortLongFile(File file, long defaultVal) {
    this.file = file;
    this.defaultVal = defaultVal;
  }
  
  /**
   * 获取当前存储的long数值。
   * @return 当前存储的数值
   * @throws IOException 文件读取失败时抛出异常
   */
  public long get() throws IOException {
    lazyOpen();
    return value;
  }

  /**
   * 设置并持久化新的数值到文件。
   * @param newVal 需要存储的新数值
   * @throws IOException 文件写入失败时抛出异常
   */
  public void set(long newVal) throws IOException {
    lazyOpen();
    // 清空缓冲区准备写入新值
    buf.clear();
    // 将long值写入缓冲区
    buf.putLong(newVal);
    // 翻转缓冲区准备读取写入
    buf.flip();
    // 将缓冲区内容完全写入文件通道
    IOUtils.writeFully(ch, buf, 0);
    // 更新内存中的数值缓存
    value = newVal;
  }
  
  /**
   * 延迟打开文件并加载初始数值，仅在首次操作时执行。
   * @throws IOException 文件读取或打开失败时抛出异常
   */
  private void lazyOpen() throws IOException {
    if (ch != null) {
      return;
    }

    // 加载磁盘文件中的当前值
    byte[] data = null;
    try {
      // 读取整个文件内容到字节数组
      data = Files.toByteArray(file);
    } catch (FileNotFoundException fnfe) {
      // 文件不存在符合预期，后续使用默认值
    }

    if (data != null && data.length != 0) {
      // 校验文件长度是否匹配long类型大小
      if (data.length != Longs.BYTES) {
        throw new IOException("File " + file + " had invalid length: " +
            data.length);
      }
      // 从字节数组解析出long数值
      value = Longs.fromByteArray(data);
    } else {
      // 文件不存在或为空，使用默认值
      value = defaultVal;
    }
    
    // 打开文件供后续写入操作
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    try {
      ch = raf.getChannel();
    } finally {
      if (ch == null) {
        IOUtils.closeStream(raf);
      }
    }
  }
  
  @Override
  /**
   * 关闭文件通道，释放资源。
   * @throws IOException 关闭失败时抛出异常
   */
  public void close() throws IOException {
    if (ch != null) {
      ch.close();
      ch = null;
    }
  }
}