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

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

/**
 * 文件/目录条目状态枚举，设计为可通过整数序列化传输，枚举的ordinal值即为序列化后的传输值。
 * 用于Manifest提交协议中记录文件系统路径的状态信息。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum EntryStatus {

  /** 未知状态 */
  unknown,
  /** 路径不存在 */
  not_found,
  /** 是普通文件 */
  file,
  /** 是已存在目录 */
  dir,
  /** 是本次任务创建的目录 */
  created_dir;

  /**
   * 将序列化后的整数转换为对应的EntryStatus枚举值。
   * 超出范围的数值统一转换为unknown未知状态。
   * @param type 序列化后的整数类型值
   * @return 对应的EntryStatus枚举值
   */
  public static EntryStatus toEntryStatus(int type) {
    switch (type) {
    case 1:
      return not_found;
    case 2:
      return file;
    case 3:
      return dir;
    case 4:
      return created_dir;
    case 0:
    default:
      return unknown;
    }
  }


  /**
   * 根据getFileStatus查询结果或目录列表条目，推断路径对应的状态。
   * null参数会被映射为not_found状态。
   * @param st 文件系统查询得到的FileStatus，可为null
   * @return 对应的状态枚举值
   */
  public static EntryStatus toEntryStatus(@Nullable FileStatus st) {

    if (st == null) {
      return not_found;
    }
    if (st.isDirectory()) {
      return dir;
    }
    if (st.isFile()) {
      return file;
    }
    return unknown;
  }


}