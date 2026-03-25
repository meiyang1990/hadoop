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
package org.apache.hadoop.mapred.nativetask.util;

import org.apache.hadoop.classification.InterfaceAudience;

import java.nio.charset.StandardCharsets;

/**
 * 为本地任务提供带读写位置的字节缓冲区，支持基本数据类型和字符串的序列化读写
 * 用于本地任务和Java任务之间的数据序列化传输
 */
@InterfaceAudience.Private
public class ReadWriteBuffer {
  private byte[] _buff;
  private int _writePoint;
  private int _readPoint;
  final static int CACHE_LINE_SIZE = 16;

  /**
   * 构造指定初始容量的读写缓冲区
   * @param length 初始缓冲区容量
   */
  public ReadWriteBuffer(int length) {
    if (length > 0) {
      _buff = new byte[length];
    }
  }

  /**
   * 构造使用默认初始容量的读写缓冲区
   */
  public ReadWriteBuffer() {
    _buff = new byte[CACHE_LINE_SIZE];
  }

  /**
   * 使用已有字节数组构造读写缓冲区，重置读写位置
   * @param bytes 用于读写的字节数组
   */
  public ReadWriteBuffer(byte[] bytes) {
    _buff = bytes;
    _writePoint = 0;
    _readPoint = 0;
  }

  /**
   * 重置缓冲区为新的字节数组，并清零读写位置
   * @param newBuff 新的底层字节数组
   */
  public void reset(byte[] newBuff) {
    _buff = newBuff;
    _writePoint = 0;
    _readPoint = 0;
  }

  /**
   * 设置读位置偏移量
   * @param pos 新的读位置
   */
  public void setReadPoint(int pos) {
    _readPoint = pos;
  }

  /**
   * 设置写位置偏移量
   * @param pos 新的写位置
   */
  public void setWritePoint(int pos) {
    _writePoint = pos;
  }

  /**
   * 获取底层存储的字节数组
   * @return 底层字节缓冲区
   */
  public byte[] getBuff() {
    return _buff;
  }

  /**
   * 获取当前写位置偏移量
   * @return 当前写位置
   */
  public int getWritePoint() {
    return _writePoint;
  }

  /**
   * 获取当前读位置偏移量
   * @return 当前读位置
   */
  public int getReadPoint() {
    return _readPoint;
  }

  /**
   * 写入一个int类型值到缓冲区
   * @param v 要写入的int值
   */
  public void writeInt(int v) {
    checkWriteSpaceAndResizeIfNecessary(4);

    // 按小端字节序写入四个字节
    _buff[_writePoint + 0] = (byte) ((v >>> 0) & 0xFF);
    _buff[_writePoint + 1] = (byte) ((v >>> 8) & 0xFF);
    _buff[_writePoint + 2] = (byte) ((v >>> 16) & 0xFF);
    _buff[_writePoint + 3] = (byte) ((v >>> 24) & 0xFF);

    _writePoint += 4;
  }

  /**
   * 写入一个long类型值到缓冲区
   * @param v 要写入的long值
   */
  public void writeLong(long v) {
    checkWriteSpaceAndResizeIfNecessary(8);

    // 按小端字节序写入八个字节
    _buff[_writePoint + 0] = (byte) (v >>> 0);
    _buff[_writePoint + 1] = (byte) (v >>> 8);
    _buff[_writePoint + 2] = (byte) (v >>> 16);
    _buff[_writePoint + 3] = (byte) (v >>> 24);
    _buff[_writePoint + 4] = (byte) (v >>> 32);
    _buff[_writePoint + 5] = (byte) (v >>> 40);
    _buff[_writePoint + 6] = (byte) (v >>> 48);
    _buff[_writePoint + 7] = (byte) (v >>> 56);

    _writePoint += 8;
  }

  /**
   * 写入指定长度的字节数组到缓冲区，先写入长度再写入数据
   * @param b 源字节数组
   * @param off 源数组起始偏移
   * @param len 要写入的长度
   */
  public void writeBytes(byte b[], int off, int len) {
    writeInt(len);
    checkWriteSpaceAndResizeIfNecessary(len);
    System.arraycopy(b, off, _buff, _writePoint, len);
    _writePoint += len;
  }

  /**
   * 从缓冲区读取一个int类型值
   * @return 读取到的int值
   */
  public int readInt() {
    // 按小端字节序读取四个字节组装int
    final int ch4 = 0xff & (_buff[_readPoint + 0]);
    final int ch3 = 0xff & (_buff[_readPoint + 1]);
    final int ch2 = 0xff & (_buff[_readPoint + 2]);
    final int ch1 = 0xff & (_buff[_readPoint + 3]);
    _readPoint += 4;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /**
   * 从缓冲区读取一个long类型值
   * @return 读取到的long值
   */
  public long readLong() {
    final long result =
      ((_buff[_readPoint + 0] & 255) << 0) +
      ((_buff[_readPoint + 1] & 255) << 8) +
      ((_buff[_readPoint + 2] & 255) << 16) +
      ((long) (_buff[_readPoint + 3] & 255) << 24) +
      ((long) (_buff[_readPoint + 4] & 255) << 32) +
      ((long) (_buff[_readPoint + 5] & 255) << 40) +
      ((long) (_buff[_readPoint + 6] & 255) << 48) +
      (((long) _buff[_readPoint + 7] << 56));

    _readPoint += 8;
    return result;
  }

  /**
   * 从缓冲区读取字节数组，先读取长度再读取内容
   * @return 读取到的字节数组
   */
  public byte[] readBytes() {
    final int length = readInt();
    final byte[] result = new byte[length];
    System.arraycopy(_buff, _readPoint, result, 0, length);
    _readPoint += length;
    return result;
  }

  /**
   * 将字符串按UTF-8编码写入缓冲区
   * @param str 要写入的字符串
   */
  public void writeString(String str) {
    final byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    writeBytes(bytes, 0, bytes.length);
  }

  /**
   * 从缓冲区读取UTF-8编码的字符串
   * @return 读取到的字符串
   */
  public String readString() {
    final byte[] bytes = readBytes();
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * 检查剩余空间是否足够写入，不够则自动扩容缓冲区
   * @param toBeWritten 需要写入的字节数
   */
  private void checkWriteSpaceAndResizeIfNecessary(int toBeWritten) {

    // 空间足够不需要扩容
    if (_buff.length - _writePoint >= toBeWritten) {
      return;
    }
    // 计算新容量，至少为默认缓存行大小
    final int newLength = (toBeWritten + _writePoint > CACHE_LINE_SIZE) ?
      (toBeWritten + _writePoint) : CACHE_LINE_SIZE;
    final byte[] newBuff = new byte[newLength];
    // 复制原有数据到新缓冲区
    System.arraycopy(_buff, 0, newBuff, 0, _writePoint);
    _buff = newBuff;
  }

};