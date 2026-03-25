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
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * @file LayoutFlags.java
 * @brief HDFS布局特性标志容器，独立于布局版本标识FSImage和编辑日志支持的功能特性
 *
 * 核心职责：维护HDFS元数据布局支持的可选功能特性标志，允许在不改变整体布局版本的情况下
 * 声明额外功能支持，同时提供IO读写序列化能力实现标志的持久化。
 * 注意：所有以'test'开头的标志预留给单元测试使用。
 */
@InterfaceAudience.Private
public class LayoutFlags {

  /**
   * 从输入流读取并校验布局特性标志段
   *
   * 检查输入流中特性标志段长度，当前版本HDFS不支持非零长度的特性标志段，
   * 如果读取到非零长度会抛出异常提示用户升级软件。
   *
   * @param in            输入流，用于读取布局特性标志数据
   * @throws IOException  读取失败或特性标志长度不合法时抛出异常
   */
  public static void read(DataInputStream in) throws IOException {
    // 读取特性标志段长度
    int length = in.readInt();
    if (length < 0) {
      // 长度为负数，数据格式非法
      throw new IOException("The length of the feature flag section " +
          "was negative at " + length + " bytes.");
    } else if (length > 0) {
      // 当前版本不支持任何特性标志，提示升级软件
      throw new IOException("Found feature flags which we can't handle. " +
          "Please upgrade your software.");
    }
  }

  private LayoutFlags() {
  }

  /**
   * 将布局特性标志写入输出流
   *
   * 当前版本HDFS不支持任何特性标志，固定写入长度为0的标志段。
   *
   * @param out           输出流，用于写入布局特性标志数据
   * @throws IOException  写入失败时抛出异常
   */
  public static void write(DataOutputStream out) throws IOException {
    out.writeInt(0);
  }
}