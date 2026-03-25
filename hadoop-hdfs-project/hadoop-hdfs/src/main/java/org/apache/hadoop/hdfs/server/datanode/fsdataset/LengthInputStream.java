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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * 带长度信息的输入流装饰器，用于HDFS DataNode数据块读取场景，包装底层输入流并记录流总长度
 */
public class LengthInputStream extends FilterInputStream {

  private final long length;

  /**
   * 构造带长度信息的装饰输入流
   * @param in 底层被包装的输入流
   * @param length 输入流总长度
   */
  public LengthInputStream(InputStream in, long length) {
    super(in);
    this.length = length;
  }

  /**
   * 获取输入流总长度
   * @return 输入流总长度（字节数）
   */
  public long getLength() {
    return length;
  }
  
  /**
   * 获取被包装的原始输入流
   * @return 底层原始输入流
   */
  public InputStream getWrappedStream() {
    return in;
  }
}