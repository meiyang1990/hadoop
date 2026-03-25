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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream; 
import java.io.FilterOutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

/**
 * IFile校验和输出流
 * 为MapReduce中间文件IFile自动计算数据校验和，在流关闭时将校验和追加到文件末尾
 * 用于MapReduce Shuffle阶段的中间结果文件写入，保障数据完整性
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IFileOutputStream extends FilterOutputStream {
  /**
   * 用于计算数据校验和的对象
   */
  private final DataChecksum sum;
  private byte[] barray;
  private boolean closed = false;
  private boolean finished = false;

  /**
   * 构造IFile校验和输出流，包装底层输出流
   * @param out 底层输出流
   */
  public IFileOutputStream(OutputStream out) {
    super(out);
    // 创建CRC32类型的校验和计算器
    sum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32,
        Integer.MAX_VALUE);
    // 初始化存储校验值的字节数组
    barray = new byte[sum.getChecksumSize()];
  }
  
  @Override
  public void close() throws IOException {
    // 避免重复关闭
    if (closed) {
      return;
    }
    closed = true;
    try {
      // 完成写入，追加校验和
      finish();
    } finally {
      // 关闭底层输出流
      IOUtils.closeStream(out);
    }
  }

  /**
   * 完成输出流写入，将计算好的校验和写入到文件末尾
   * 不会关闭底层输出流
   * @throws IOException IO异常
   */
  public void finish() throws IOException {
    // 避免重复执行finish
    if (finished) {
      return;
    }
    finished = true;
    // 将校验值写入到字节数组
    sum.writeValue(barray, 0, false);
    // 将校验和写入到底层流
    out.write (barray, 0, sum.getChecksumSize());
    // 刷新缓冲区
    out.flush();
  }

  /**
   * 批量写入字节数组，同时更新校验和
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // 更新校验和计算
    sum.update(b, off,len);
    // 写入底层输出流
    out.write(b,off,len);
  }
 
  @Override
  public void write(int b) throws IOException {
    // 将int转为单字节
    barray[0] = (byte) (b & 0xFF);
    // 调用批量写入方法，同时更新校验和
    write(barray,0,1);
  }

}