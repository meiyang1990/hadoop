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

package org.apache.hadoop.hdfs.server.datanode;

/**
 * 数据块分块校验和容器，存储对应数据块的校验和字节数组以及校验和覆盖的数据长度
 * 用于HDFS DataNode处理数据块校验，当数据块长度不是分块大小整数倍时，记录最后不完整分块的校验信息
 * 例如：数据长度为1023，分块大小为512，该校验和覆盖最后一个分块即512-1023字节
 */
public class ChunkChecksum {
  private final long dataLength;
  // 校验和字节数组，若不可用则为null
  private final byte[] checksum;

  /**
   * 构造分块校验和对象
   * @param dataLength 校验和覆盖的数据总长度
   * @param checksum 校验和字节数组
   */
  public ChunkChecksum(long dataLength, byte[] checksum) {
    this.dataLength = dataLength;
    this.checksum = checksum;
  }

  /**
   * 获取校验和覆盖的数据总长度
   * @return 数据总长度（字节）
   */
  public long getDataLength() {
    return dataLength;
  }

  /**
   * 获取分块校验和字节数组
   * @return 校验和字节数组，不可用时返回null
   */
  public byte[] getChecksum() {
    return checksum;
  }
}