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
package org.apache.hadoop.mapreduce.lib.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * MapReduce Join操作基于流缓存的可重置迭代器实现。
 * 使用字节数组存储添加到迭代器中的元素，支持重置、重放迭代操作，
 * 用于Reduce端连接操作中缓存来自不同输入分片的数据。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StreamBackedIterator<X extends Writable>
    implements ResetableIterator<X> {

  /**
   * 支持重置流位置的可重放字节输入流，扩展ByteArrayInputStream实现
   */
  private static class ReplayableByteInputStream extends ByteArrayInputStream {
    public ReplayableByteInputStream(byte[] arr) {
      super(arr);
    }
    /**
     * 将流重置到初始位置，准备重新读取
     */
    public void resetStream() {
      mark = 0;
      reset();
    }
  }

  // 缓存输出字节流，用于存储序列化后的Writable元素
  private ByteArrayOutputStream outbuf = new ByteArrayOutputStream();
  // 缓存输出数据流，用于写入Writable对象
  private DataOutputStream outfbuf = new DataOutputStream(outbuf);
  // 可重放字节输入流，用于读取缓存的数据
  private ReplayableByteInputStream inbuf;
  // 数据输入流，用于反序列化Writable对象
  private DataInputStream infbuf;

  /**
   * 构造空的StreamBacked迭代器
   */
  public StreamBackedIterator() { }

  /**
   * 检查迭代器是否还有元素可以读取
   * @return true 还有未读取元素，false 已读取完所有元素
   */
  public boolean hasNext() {
    return infbuf != null && inbuf.available() > 0;
  }

  /**
   * 获取迭代器下一个元素，反序列化到给定对象中
   * @param val 用于接收反序列化结果的Writable对象
   * @return 成功读取元素返回true，无元素返回false
   * @throws IOException 反序列化IO异常
   */
  public boolean next(X val) throws IOException {
    if (hasNext()) {
      inbuf.mark(0);
      val.readFields(infbuf);
      return true;
    }
    return false;
  }

  /**
   * 重新读取当前元素，用于迭代器重置后重放数据
   * @param val 用于接收反序列化结果的Writable对象
   * @return 成功重放返回true，无元素返回false
   * @throws IOException 反序列化IO异常
   */
  public boolean replay(X val) throws IOException {
    inbuf.reset();
    if (0 == inbuf.available())
      return false;
    val.readFields(infbuf);
    return true;
  }

  /**
   * 重置迭代器到初始位置，完成缓存后切换到读取模式，准备重新遍历
   */
  public void reset() {
    if (null != outfbuf) {
      // 将缓存的所有字节包装为输入流，准备读取
      inbuf = new ReplayableByteInputStream(outbuf.toByteArray());
      infbuf =  new DataInputStream(inbuf);
      outfbuf = null; // 缓存完成，不再接收新元素
    }
    inbuf.resetStream();
  }

  /**
   * 将元素序列化添加到缓存中
   * @param item 需要添加到缓存的Writable元素
   * @throws IOException 序列化IO异常
   */
  public void add(X item) throws IOException {
    item.write(outfbuf);
  }

  /**
   * 关闭迭代器，释放所有打开的流资源
   * @throws IOException 关闭流时的IO异常
   */
  public void close() throws IOException {
    if (null != infbuf)
      infbuf.close();
    if (null != outfbuf)
      outfbuf.close();
  }

  /**
   * 清空迭代器中的所有缓存数据，重置到初始状态，可重新添加元素
   */
  public void clear() {
    if (null != inbuf)
      inbuf.resetStream();
    outbuf.reset();
    outfbuf = new DataOutputStream(outbuf);
  }
}