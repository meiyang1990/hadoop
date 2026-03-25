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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/**
 * 文件系统状态不一致且不可恢复时抛出的异常
 * 通常出现在HDFS元数据存储目录损坏或状态异常场景
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class InconsistentFSStateException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造函数，基于异常目录和描述信息创建异常
   * @param dir 状态异常的目录
   * @param descr 异常描述信息
   */
  public InconsistentFSStateException(File dir, String descr) {
    super("Directory " + getFilePath(dir)
          + " is in an inconsistent state: " + descr);
  }

  /**
   * 构造函数，基于异常目录、描述信息和原始异常创建异常
   * @param dir 状态异常的目录
   * @param descr 异常描述信息
   * @param ex 原始异常
   */
  public InconsistentFSStateException(File dir, String descr, Throwable ex) {
    this(dir, descr + "\n" + StringUtils.stringifyException(ex));
  }
  
  /**
   * 获取文件的规范化路径，获取失败则返回普通路径
   * @param dir 需要获取路径的目录对象
   * @return 目录的完整路径字符串
   */
  private static String getFilePath(File dir) {
    try {
      // 尝试获取规范化绝对路径
      return dir.getCanonicalPath();
    } catch(IOException e) {}
    // 获取失败则返回原始路径
    return dir.getPath();
  }
}