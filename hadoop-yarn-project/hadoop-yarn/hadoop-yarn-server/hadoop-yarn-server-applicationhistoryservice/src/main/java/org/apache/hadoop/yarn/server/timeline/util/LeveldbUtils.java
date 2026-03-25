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

/**
 * LevelDB工具类，为时间线服务存储提供键构建、解析、数据库加载修复等通用能力
 */
package org.apache.hadoop.yarn.server.timeline.util;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.Time;

import java.io.File;
import java.io.IOException;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;

/**
 * LevelDB工具类，提供时间线服务存储相关的工具方法
 */
public class LeveldbUtils {

  private static final String BACKUP_EXT = ".backup-";
  private static final Logger LOG = LoggerFactory
      .getLogger(LeveldbUtils.class);

  /** 时间线服务LevelDB键构建工具类，用于构建层级化的存储键 */
  public static class KeyBuilder {
    /** 单个键最多支持的子键数量 */
    private static final int MAX_NUMBER_OF_KEY_ELEMENTS = 10;
    private byte[][] b;
    private boolean[] useSeparator;
    private int index;
    private int length;

    public KeyBuilder(int size) {
      b = new byte[size][];
      useSeparator = new boolean[size];
      index = 0;
      length = 0;
    }

    /** 创建默认容量的键构建器 */
    public static KeyBuilder newInstance() {
      return new KeyBuilder(MAX_NUMBER_OF_KEY_ELEMENTS);
    }

    /** 
     * 创建指定最大子键数量的键构建器
     * @param size 最大可添加的子键数量
     * @return 新建的键构建器
     */
    public static KeyBuilder newInstance(final int size) {
      return new KeyBuilder(size);
    }

    /** 添加字符串类型子键，自动添加分隔符 */
    public KeyBuilder add(String s) {
      return add(s.getBytes(UTF_8), true);
    }

    /** 添加字节数组类型子键，默认不添加分隔符 */
    public KeyBuilder add(byte[] t) {
      return add(t, false);
    }

    /** 
     * 添加字节数组类型子键，可指定是否添加分隔符 
     * @param t 子键字节数组
     * @param sep 是否添加分隔符
     * @return 当前键构建器实例
     */
    public KeyBuilder add(byte[] t, boolean sep) {
      b[index] = t;
      useSeparator[index] = sep;
      length += t.length;
      if (sep) {
        length++;
      }
      index++;
      return this;
    }

    /** 
     * 构建最终键字节数组，不保留末尾分隔符 
     * @return 组合后的完整键字节数组
     */
    public byte[] getBytes() {
      // 计算最终有效长度，去掉末尾多余的分隔符
      int bytesLength = length;
      if (useSeparator[index - 1]) {
        bytesLength = length - 1;
      }
      byte[] bytes = new byte[bytesLength];
      int curPos = 0;
      // 逐段拷贝子键到结果数组
      for (int i = 0; i < index; i++) {
        System.arraycopy(b[i], 0, bytes, curPos, b[i].length);
        curPos += b[i].length;
        // 仅在非最后一个子键后添加分隔符
        if (i < index - 1 && useSeparator[i]) {
          bytes[curPos++] = 0x0;
        }
      }
      return bytes;
    }

    /** 
     * 构建范围查找用键字节数组，保留所有分隔符 
     * @return 用于前缀查找的键字节数组
     */
    public byte[] getBytesForLookup() {
      byte[] bytes = new byte[length];
      int curPos = 0;
      // 逐段拷贝所有子键和分隔符
      for (int i = 0; i < index; i++) {
        System.arraycopy(b[i], 0, bytes, curPos, b[i].length);
        curPos += b[i].length;
        if (useSeparator[i]) {
          bytes[curPos++] = 0x0;
        }
      }
      return bytes;
    }
  }

  /** LevelDB键解析工具类，用于从组合键中解析出各子键 */
  public static class KeyParser {
    private final byte[] b;
    private int offset;

    public KeyParser(final byte[] b, final int offset) {
      this.b = b;
      this.offset = offset;
    }

    /** 
     * 解析下一个字符串子键，直到遇到分隔符 
     * @return 解析出的字符串
     * @throws IOException 解析越界时抛出异常
     */
    public String getNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException(
            "tried to read nonexistent string from byte array");
      }
      int i = 0;
      // 查找下一个分隔符位置
      while (offset + i < b.length && b[offset + i] != 0x0) {
        i++;
      }
      // 构造字符串并移动偏移量
      String s = new String(b, offset, i, UTF_8);
      offset = offset + i + 1;
      return s;
    }

    /** 
     * 跳过下一个字符串子键，移动偏移量到下一个子键起始位置 
     * @throws IOException 解析越界时抛出异常
     */
    public void skipNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException("tried to read nonexistent string from byte array");
      }
      // 跳过直到遇到分隔符
      while (offset < b.length && b[offset] != 0x0) {
        ++offset;
      }
      ++offset;
    }

    /** 
     * 读取下一个8字节长整型 
     * @return 解析出的长整型值
     * @throws IOException 解析越界时抛出异常
     */
    public long getNextLong() throws IOException {
      if (offset + 8 >= b.length) {
        throw new IOException("byte array ran out when trying to read long");
      }
      // 读取反向排序的长整型
      long value = readReverseOrderedLong(b, offset);
      offset += 8;
      return value;
    }

    public int getOffset() {
      return offset;
    }

    /** 
     * 获取剩余未解析的全部字节 
     * @return 剩余字节的拷贝
     */
    public byte[] getRemainingBytes() {
      byte[] bytes = new byte[b.length - offset];
      System.arraycopy(b, offset, bytes, 0, b.length - offset);
      return bytes;
    }
  }

  /**
   * 检查字节数组是否以指定前缀开头
   * @param prefix 前缀字节数组
   * @param prefixlen 前缀长度
   * @param b 待检查的字节数组
   * @return 匹配返回true，否则返回false
   */
  public static boolean prefixMatches(byte[] prefix, int prefixlen,
      byte[] b) {
    if (b.length < prefixlen) {
      return false;
    }
    return WritableComparator.compareBytes(prefix, 0, prefixlen, b, 0,
        prefixlen) == 0;
  }

  /** LevelDB目录默认权限掩码 */
  public static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  /**
   * 加载LevelDB数据库，如果加载失败则自动备份并修复
   * @param factory LevelDB工厂实例
   * @param dbPath 数据库路径
   * @param options LevelDB选项
   * @return 打开的数据库实例
   * @throws IOException 打开或修复失败时抛出异常
   */
  public static DB loadOrRepairLevelDb(JniDBFactory factory, Path dbPath, Options options)
      throws IOException {
    DB db;
    try{
      // 尝试正常打开数据库
      db = factory.open(new File(dbPath.toString()), options);
    } catch (IOException ioe){
      // 打开失败，备份损坏数据库后进行修复
      File dbFile = new File(dbPath.toString());
      // 备份路径带时间戳，避免覆盖
      File dbBackupPath = new File(
          dbPath.toString() + BACKUP_EXT + Time.monotonicNow());
      LOG.warn("Incurred exception while loading LevelDb database. Backing " +
          "up at "+ dbBackupPath, ioe);
      // 拷贝整个数据库目录到备份位置
      FileUtils.copyDirectory(dbFile, dbBackupPath);
      // 执行修复
      factory.repair(dbFile, options);
      // 重新打开修复后的数据库
      db = factory.open(dbFile, options);
    }
    return db;
  }

}