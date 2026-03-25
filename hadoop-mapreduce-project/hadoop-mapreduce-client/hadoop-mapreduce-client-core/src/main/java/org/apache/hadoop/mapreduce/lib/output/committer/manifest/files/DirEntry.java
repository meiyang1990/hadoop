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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.marshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.unmarshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.verify;

/**
 * 任务清单文件中的目录条目实体，用于记录作业输出过程中创建的目录信息
 * 使用短JSON字段名减小清单文件体积，仅基于目录路径计算哈希和相等性判断
 * 支持Java序列化、JSON序列化和Hadoop Writable三种序列化方式
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class DirEntry implements Serializable, Writable {

  private static final long serialVersionUID = 5658520530209859765L;

  /**
   * 目标目录路径，JSON字段名使用短名'd'减小文件体积
   */
  @JsonProperty("d")
  private String dir;

  /**
   * 目录状态类型，任务提交探测时获取的条目状态，JSON字段名使用短名't'减小文件体积
   */
  @JsonProperty("t")
  private int type;

  /**
   * 目录在遍历树中的层级，JSON字段名使用短名'l'减小文件体积
   */
  @JsonProperty("l")
  private int level;

  /**
   * 无参构造器，供Jackson序列化/反序列化和Writable机制使用，不得删除
   */
  private DirEntry() {
  }

  /**
   * 构造目录条目对象
   * @param dir 目标目录路径字符串
   * @param type 目录状态类型编号
   * @param level 目录在遍历树中的层级
   */
  public DirEntry(
      final String dir,
      final int type,
      final int level) {
    this.dir = requireNonNull(dir);
    this.type = type;
    this.level = level;
  }

  /**
   * 构造目录条目对象，接收Path类型参数
   * @param dir 目标目录路径
   * @param type 目录状态类型编号
   * @param level 目录在遍历树中的层级
   */
  public DirEntry(
      final Path dir,
      final int type,
      final int level) {
    this(marshallPath(dir), type, level);
  }

  public void setDir(final String dir) {
    this.dir = dir;
  }

  public String getDir() {
    return dir;
  }

  /**
   * 获取反序列化后的目标目录Path对象，该字段不参与JSON序列化
   * @return 目标目录Path对象
   */
  @JsonIgnore
  public Path getDestPath() {
    return unmarshallPath(dir);
  }

  public int getType() {
    return type;
  }

  public void setType(final int type) {
    this.type = type;
  }

  public void setLevel(final int level) {
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  /**
   * 获取目录状态枚举，该字段不参与JSON序列化
   * @return 目录状态枚举
   */
  @JsonIgnore
  public EntryStatus getStatus() {
    return EntryStatus.toEntryStatus(type);
  }

  /**
   * 设置目录状态，该字段不参与JSON序列化
   * @param status 目录状态枚举
   */
  @JsonIgnore
  public void setStatus(EntryStatus status) {
    setType(status.ordinal());
  }

  /**
   * 验证当前目录条目的数据合法性，检查必填字段是否符合要求
   * @throws IOException 验证失败时抛出异常
   */
  public void validate() throws IOException {
    final String s = toString();
    verify(dir != null && dir.length() > 0,
        "destination path is missing from " + s);
    verify(type >= 0,
        "Invalid type in " + s);
    verify(level >= 0,
        "Invalid level in " + s);
  }

  @Override
  public String toString() {
    return "DirEntry{" +
        "dir='" + dir + '\'' +
        ", type=" + type +
        ", level=" + level +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DirEntry dirEntry = (DirEntry) o;
    return dir.equals(dirEntry.dir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dir);
  }

  /**
   * 将目录条目写入Hadoop序列化输出
   * @param out 数据输出流
   * @throws IOException 写入失败时抛出异常
   */
  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeUTF(dir);
    out.writeInt(type);
    out.writeInt(level);
  }

  /**
   * 从Hadoop序列化输入读取目录条目数据
   * @param in 数据输入流
   * @throws IOException 读取失败时抛出异常
   */
  @Override
  public void readFields(final DataInput in) throws IOException {
    dir = in.readUTF();
    type = in.readInt();
    level = in.readInt();
  }

  /**
   * 工厂方法：创建目录条目，接收Path类型参数和状态类型编号
   * @param dest 目标目录路径
   * @param type 目录状态类型编号
   * @param level 目录在遍历树中的层级
   * @return 新的目录条目对象
   */
  public static DirEntry dirEntry(Path dest, int type, int level) {
    return new DirEntry(dest, type, level);
  }

  /**
   * 工厂方法：创建目录条目，接收Path类型参数和状态枚举
   * @param dest 目标目录路径
   * @param type 目录状态枚举
   * @param level 目录在遍历树中的层级
   * @return 新的目录条目对象
   */
  public static DirEntry dirEntry(Path dest, EntryStatus type, int level) {
    return dirEntry(dest, type.ordinal(), level);
  }

}