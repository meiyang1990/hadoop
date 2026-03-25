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

import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;

/** 
 * 字节数组包装类，用于将字节数组作为HashMap的键使用
 * 原生字节数组的hashCode基于对象地址，无法正确用作哈希表键，该类重写了equals和hashCode方法，基于字节内容实现正确的哈希键语义
 */
@InterfaceAudience.Private
public class ByteArray {
  private int hash = 0; // 缓存哈希值，避免重复计算
  private final byte[] bytes;
  
  /**
   * 构造方法，包装给定的字节数组
   * @param bytes 被包装的字节数组
   */
  public ByteArray(byte[] bytes) {
    this.bytes = bytes;
  }
  
  /**
   * 获取被包装的原始字节数组
   * @return 原始字节数组
   */
  public byte[] getBytes() {
    return bytes;
  }
  
  @Override
  public int hashCode() {
    // 懒计算哈希值，命中缓存直接返回
    if (hash == 0) {
      hash = Arrays.hashCode(bytes);
    }
    return hash;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ByteArray)) {
      return false;
    }
    // 基于字节数组内容逐字节比较相等性
    return Arrays.equals(bytes, ((ByteArray)o).bytes);
  }
}