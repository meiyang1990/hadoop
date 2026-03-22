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

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IFile 校验和输入流，用于读取 MapReduce 中间文件并验证数据完整性。
 * 配合 {@link IFileOutputStream} 使用，验证写入的IFile数据校验和是否正确。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IFileInputStream extends InputStream {
  
  private final InputStream in; // 待校验的底层输入流
  private final FileDescriptor inFd; // 底层文件描述符（如果可获取），用于预读
  private final long length; // 输入流总长度（包含校验和字节）
  private final long dataLength; // 实际数据长度（不包含末尾校验和）
  private DataChecksum sum; // 数据校验和计算器
  private long currentOffset = 0; // 当前读取位置偏移量
  private final byte b[] = new byte[1]; // 单字节读取缓存
  private byte csum[] = null; // 存储文件末尾存储的校验和
  private int checksumSize; // 校验和字节长度

  private ReadaheadRequest curReadahead = null; // 当前预读请求
  private ReadaheadPool raPool = ReadaheadPool.getInstance(); // 全局预读池实例
  private boolean readahead; // 是否开启预读
  private int readaheadLength; // 单次预读长度

  public static final Logger LOG =
      LoggerFactory.getLogger(IFileInputStream.class);

  private boolean disableChecksumValidation = false; // 是否禁用校验和验证
  
  /**
   * 构造IFile校验和输入流
   * @param in 待读取的底层输入流
   * @param len 包含校验和的输入流总长度
   * @param conf Hadoop配置对象，用于读取预读相关配置
   */
  public IFileInputStream(InputStream in, long len, Configuration conf) {
    this.in = in;
    this.inFd = getFileDescriptorIfAvail(in);
    sum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 
        Integer.MAX_VALUE);
    checksumSize = sum.getChecksumSize();
    length = len;
    dataLength = length - checksumSize;

    conf = (conf != null) ? conf : new Configuration();
    readahead = conf.getBoolean(MRConfig.MAPRED_IFILE_READAHEAD,
        MRConfig.DEFAULT_MAPRED_IFILE_READAHEAD);
    readaheadLength = conf.getInt(MRConfig.MAPRED_IFILE_READAHEAD_BYTES,
        MRConfig.DEFAULT_MAPRED_IFILE_READAHEAD_BYTES);

    doReadahead();
  }

  /**
   * 尝试从输入流中获取文件描述符，用于预读优化
   * @param in 输入流对象
   * @return 若可获取则返回文件描述符，否则返回null
   */
  private static FileDescriptor getFileDescriptorIfAvail(InputStream in) {
    FileDescriptor fd = null;
    try {
      if (in instanceof HasFileDescriptor) {
        fd = ((HasFileDescriptor)in).getFileDescriptor();
      } else if (in instanceof FileInputStream) {
        fd = ((FileInputStream)in).getFD();
      }
    } catch (IOException e) {
      LOG.info("Unable to determine FileDescriptor", e);
    }
    return fd;
  }

  /**
   * 关闭输入流，关闭前会读完所有数据完成校验和验证（如果未读完）
   */
  @Override
  public void close() throws IOException {

    if (curReadahead != null) {
      curReadahead.cancel();
    }
    if (currentOffset < dataLength) {
      byte[] t = new byte[Math.min((int)
            (Integer.MAX_VALUE & (dataLength - currentOffset)), 32 * 1024)];
      while (currentOffset < dataLength) {
        int n = read(t, 0, t.length);
        if (0 == n) {
          throw new EOFException("Could not validate checksum");
        }
      }
    }
    in.close();
  }
  
  @Override
  public long skip(long n) throws IOException {
   throw new IOException("Skip not supported for IFileInputStream");
  }
  
  /**
   * 获取当前读取位置（不超过实际数据长度）
   * @return 当前读取偏移量
   */
  public long getPosition() {
    return (currentOffset >= dataLength) ? dataLength : currentOffset;
  }
  
  /**
   * 获取校验和字节长度
   * @return 校验和字节数
   */
  public long getSize() {
    return checksumSize;
  }
  
  /**
   * 批量读取数据，不返回校验和，到达数据末尾时自动完成校验和验证
   */
  public int read(byte[] b, int off, int len) throws IOException {

    if (currentOffset >= dataLength) {
      return -1;
    }

    doReadahead();

    return doRead(b,off,len);
  }

  /**
   * 提交预读请求到预读池，优化顺序读取性能
   */
  private void doReadahead() {
    if (raPool != null && inFd != null && readahead) {
      curReadahead = raPool.readaheadStream(
          "ifile", inFd,
          currentOffset, readaheadLength, dataLength,
          curReadahead);
    }
  }

  /**
   * 批量读取数据，数据读完后会将校验和也返回给调用方
   * @param b 存放读取结果的字节数组
   * @param off 数组起始偏移量
   * @param len 最多读取字节数
   * @return 实际读取字节数，到达流末尾返回-1
   * @throws IOException 读取或校验错误时抛出IO异常
   */
  public int readWithChecksum(byte[] b, int off, int len) throws IOException {

    if (currentOffset == length) {
      return -1;
    }
    else if (currentOffset >= dataLength) {
      // 已读完所有实际数据，现在返回剩余校验和字节
      int lenToCopy = (int) (checksumSize - (currentOffset - dataLength));
      if (len < lenToCopy) {
        lenToCopy = len;
      }
      System.arraycopy(csum, (int) (currentOffset - dataLength), b, off, 
          lenToCopy);
      currentOffset += lenToCopy;
      return lenToCopy;
    }

    int bytesRead = doRead(b,off,len);

    if (currentOffset == dataLength) {
      if (len >= bytesRead + checksumSize) {
        System.arraycopy(csum, 0, b, off + bytesRead, checksumSize);
        bytesRead += checksumSize;
        currentOffset += checksumSize;
      }
    }
    return bytesRead;
  }

  /**
   * 实际执行读取和校验计算的核心方法
   * @param b 存放读取结果的字节数组
   * @param off 数组起始偏移量
   * @param len 最多读取字节数
   * @return 实际读取字节数
   * @throws IOException 读取错误或校验失败时抛出异常
   */
  private int doRead(byte[]b, int off, int len) throws IOException {
    
    // 不超过实际数据边界读取，截断读取长度
    if (currentOffset + len > dataLength) {
      len = (int) dataLength - (int)currentOffset;
    }
    
    int bytesRead = in.read(b, off, len);

    if (bytesRead < 0) {
      throw new ChecksumException("Checksum Error", 0);
    }
    
    // 更新当前读取数据的校验和计算
    sum.update(b,off,bytesRead);

    currentOffset += bytesRead;

    if (disableChecksumValidation) {
      return bytesRead;
    }
    
    if (currentOffset == dataLength) {
      // 读完所有数据，读取文件末尾存储的校验并验证
      csum = new byte[checksumSize];
      IOUtils.readFully(in, csum, 0, checksumSize);
      if (!sum.compare(csum, 0)) {
        throw new ChecksumException("Checksum Error", 0);
      }
    }
    return bytesRead;
  }


  @Override
  public int read() throws IOException {    
    b[0] = 0;
    int l = read(b,0,1);
    if (l < 0)  return l;
    
    // 将字节转为无符号int，避免符号位扩展导致负数
    int result = 0xFF & b[0];
    return result;
  }

  /**
   * 获取文件存储的校验和数组
   * @return 校验和字节数组
   */
  public byte[] getChecksum() {
    return csum;
  }

  /**
   * 禁用校验和验证，用于特殊场景
   */
  void disableChecksumValidation() {
    disableChecksumValidation = true;
  }
}