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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 多路输出流，支持将写入的数据同时转发到多个目标输出流，类似Unix的tee命令功能。
 * 用于离线edits日志查看器中同时输出到多个目的地（如文件和控制台）。
 */
public class TeeOutputStream extends OutputStream {
  // 存储所有需要输出的目标流数组
  private final OutputStream[] outs;

  /**
   * 构造多路输出流，指定多个目标输出流
   * @param outs 目标输出流数组
   */
  public TeeOutputStream(OutputStream outs[]) {
    this.outs = outs;
  }

  @Override
  public void write(int c) throws IOException {
    // 遍历所有目标流，写入单个字节
    for (OutputStream o : outs) {
     o.write(c);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    // 遍历所有目标流，写入字节数组
    for (OutputStream o : outs) {
     o.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // 遍历所有目标流，写入字节数组指定范围
    for (OutputStream o : outs) {
     o.write(b, off, len);
    }
  }

  @Override
  public void close() throws IOException {
    // 遍历所有目标流，关闭流
    for (OutputStream o : outs) {
     o.close();
    }
  }

  @Override
  public void flush() throws IOException {
    // 遍历所有目标流，刷新缓冲区
    for (OutputStream o : outs) {
     o.flush();
    }
  }
}