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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.util.EnumCounters;

/**
 * HDFS名称节点内容统计容器，用于统计不同内容类型（文件、目录、符号链接等）的数量，
 * 以及不同存储类型（SSD、DISK、ARCHIVE等）的存储空间使用量。
 * 主要用于目录空间 quota 管理、空间使用统计等场景。
 */
public class ContentCounts {
  /** 内容类型计数器 */
  private EnumCounters<Content> contents;
  /** 存储类型存储空间计数器 */
  private EnumCounters<StorageType> types;

  /**
   * ContentCounts的Builder构造器，用于灵活构建ContentCounts对象
   */
  public static class Builder {
    private EnumCounters<Content> contents;
    /** 对应存储类型已使用的存储空间 */
    private EnumCounters<StorageType> types;

    /**
     * 构造空的Builder，初始化所有计数器为0
     */
    public Builder() {
      contents = new EnumCounters<Content>(Content.class);
      types = new EnumCounters<StorageType>(StorageType.class);
    }

    /**
     * 设置文件数量
     * @param file 文件数量
     * @return 当前Builder实例
     */
    public Builder file(long file) {
      contents.set(Content.FILE, file);
      return this;
    }

    /**
     * 设置目录数量
     * @param directory 目录数量
     * @return 当前Builder实例
     */
    public Builder directory(long directory) {
      contents.set(Content.DIRECTORY, directory);
      return this;
    }

    /**
     * 设置符号链接数量
     * @param symlink 符号链接数量
     * @return 当前Builder实例
     */
    public Builder symlink(long symlink) {
      contents.set(Content.SYMLINK, symlink);
      return this;
    }

    /**
     * 设置文件总长度
     * @param length 文件总长度（字节）
     * @return 当前Builder实例
     */
    public Builder length(long length) {
      contents.set(Content.LENGTH, length);
      return this;
    }

    /**
     * 设置总存储空间（含副本）
     * @param storagespace 总存储空间（字节）
     * @return 当前Builder实例
     */
    public Builder storagespace(long storagespace) {
      contents.set(Content.DISKSPACE, storagespace);
      return this;
    }

    /**
     * 设置快照数量
     * @param snapshot 快照数量
     * @return 当前Builder实例
     */
    public Builder snapshot(long snapshot) {
      contents.set(Content.SNAPSHOT, snapshot);
      return this;
    }

    /**
     * 设置可快照目录数量
     * @param snapshotable_directory 可快照目录数量
     * @return 当前Builder实例
     */
    public Builder snapshotable_directory(long snapshotable_directory) {
      contents.set(Content.SNAPSHOTTABLE_DIRECTORY, snapshotable_directory);
      return this;
    }

    /**
     * 构造ContentCounts对象
     * @return 构建完成的ContentCounts实例
     */
    public ContentCounts build() {
      return new ContentCounts(contents, types);
    }
  }

  /**
   * 私有构造方法，由Builder调用创建实例
   * @param contents 内容类型计数器
   * @param types 存储类型计数器
   */
  private ContentCounts(EnumCounters<Content> contents,
      EnumCounters<StorageType> types) {
    this.contents = contents;
    this.types = types;
  }

  /**
   * 获取文件数量
   * @return 文件总数
   */
  public long getFileCount() {
    return contents.get(Content.FILE);
  }

  /**
   * 获取目录数量
   * @return 目录总数
   */
  public long getDirectoryCount() {
    return contents.get(Content.DIRECTORY);
  }

  /**
   * 获取符号链接数量
   * @return 符号链接总数
   */
  public long getSymlinkCount() {
    return contents.get(Content.SYMLINK);
  }

  /**
   * 获取所有文件总长度
   * @return 文件总长度，单位字节
   */
  public long getLength() {
    return contents.get(Content.LENGTH);
  }

  /**
   * 获取总存储空间使用量，包含副本占用的空间
   * @return 总存储空间，单位字节
   */
  public long getStoragespace() {
    return contents.get(Content.DISKSPACE);
  }

  /**
   * 获取快照数量
   * @return 快照总数
   */
  public long getSnapshotCount() {
    return contents.get(Content.SNAPSHOT);
  }

  /**
   * 获取可快照目录数量
   * @return 可快照目录总数
   */
  public long getSnapshotableDirectoryCount() {
    return contents.get(Content.SNAPSHOTTABLE_DIRECTORY);
  }

  /**
   * 获取所有存储类型的空间使用量数组
   * @return 按存储类型枚举顺序排列的空间使用量数组
   */
  public long[] getTypeSpaces() {
    return types.asArray();
  }

  /**
   * 获取指定存储类型的空间使用量
   * @param t 存储类型
   * @return 指定存储类型的已使用空间
   */
  public long getTypeSpace(StorageType t) {
    return types.get(t);
  }

  /**
   * 累加指定内容类型的计数
   * @param c 内容类型
   * @param val 要累加的值
   */
  public void addContent(Content c, long val) {
    contents.add(c, val);
  }

  /**
   * 累加另一个ContentCounts对象的所有计数器到当前对象
   * @param that 要累加的ContentCounts对象
   */
  public void addContents(ContentCounts that) {
    contents.add(that.contents);
    types.add(that.types);
  }

  /**
   * 累加指定存储类型的空间使用量
   * @param t 存储类型
   * @param val 要累加的空间值
   */
  public void addTypeSpace(StorageType t, long val) {
    types.add(t, val);
  }

  /**
   * 累加指定存储类型计数器的所有值到当前对象
   * @param that 要累加的存储类型计数器
   */
  public void addTypeSpaces(EnumCounters<StorageType> that) {
    this.types.add(that);
  }
}