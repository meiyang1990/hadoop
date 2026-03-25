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

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * MapReduce连接操作专用元组Writable实现，用于存储多个Writable类型元素。
 * 本类并非通用元组类型，仅为连接框架设计：它假设实例很少被持久化，依赖连接框架保证类型安全，
 * 性能和编码效率不如用户自定义的专用序列化类型，不推荐在连接框架之外使用。
 * 
 * 核心职责：为MapReduce的连接操作提供可变长度元组的序列化能力，支持稀疏存储（仅标记存在的元素）
 * @see org.apache.hadoop.io.Writable
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TupleWritable implements Writable, Iterable<Writable> {

  // 标记对应位置是否存在有效值
  protected BitSet written;
  // 存储元组的所有元素
  private Writable[] values;

  /**
   * 创建空元组，不分配存储空间
   */
  public TupleWritable() {
    written = new BitSet(0);
  }

  /**
   * 使用给定的Writable数组初始化元组，此时所有元素默认标记为未写入
   * @param vals 元组元素数组
   */
  public TupleWritable(Writable[] vals) {
    written = new BitSet(vals.length);
    values = vals;
  }

  /**
   * 判断指定位置是否存在有效值
   * @param i 要检查的位置索引
   * @return true表示该位置存在有效值，false表示不存在
   */
  public boolean has(int i) {
    return written.get(i);
  }

  /**
   * 获取元组指定位置的元素
   * @param i 位置索引
   * @return 对应位置的Writable元素
   */
  public Writable get(int i) {
    return values[i];
  }

  /**
   * 获取元组容量（可存储的最大元素数量）
   * @return 元组容量
   */
  public int size() {
    return values.length;
  }

  /**
   * 比较两个元组是否相等：需要标记位相等，且所有已存在元素都相等
   * {@inheritDoc}
   */
  public boolean equals(Object other) {
    if (other instanceof TupleWritable) {
      TupleWritable that = (TupleWritable)other;
      if (!this.written.equals(that.written)) {
        return false;
      }
      for (int i = 0; i < values.length; ++i) {
        if (!has(i)) continue;
        if (!values[i].equals(that.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * 本类未设计哈希码实现，调用会触发断言失败，仅返回written的哈希码兜底
   * @return 哈希码（实际上不会正常返回）
   */
  public int hashCode() {
    assert false : "hashCode not designed";
    return written.hashCode();
  }

  /**
   * 返回仅包含已存在元素的迭代器，迭代过程会跳过不存在元素的位置
   * @return 已存在元素的迭代器
   */
  public Iterator<Writable> iterator() {
    final TupleWritable t = this;
    return new Iterator<Writable>() {
      int bitIndex = written.nextSetBit(0);
      public boolean hasNext() {
        return bitIndex >= 0;
      }
      public Writable next() {
        int returnIndex = bitIndex;
        if (returnIndex < 0)
          throw new NoSuchElementException();
        bitIndex = written.nextSetBit(bitIndex+1);
        return t.get(returnIndex);
      }
      public void remove() {
        if (!written.get(bitIndex)) {
          throw new IllegalStateException(
            "Attempt to remove non-existent val");
        }
        written.clear(bitIndex);
      }
    };
  }

  /**
   * 将元组转换为字符串表示，格式为[元素1,元素2,...]，不存在的元素留空
   * @return 元组的字符串形式
   */
  public String toString() {
    StringBuilder buf = new StringBuilder("[");
    for (int i = 0; i < values.length; ++i) {
      buf.append(has(i) ? values[i].toString() : "");
      buf.append(",");
    }
    if (values.length != 0)
      buf.setCharAt(buf.length() - 1, ']');
    else
      buf.append(']');
    return buf.toString();
  }

  // Writable

  /** 
   * 将元组序列化到输出流，序列化格式：
   * 元素数量 -> 存在标记位 -> 每个元素的类名 -> 每个存在元素的序列化内容
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
    // 写入元组容量
    WritableUtils.writeVInt(out, values.length);
    // 写入存在标记位
    writeBitSet(out, values.length, written);
    // 写入所有元素的类名
    for (int i = 0; i < values.length; ++i) {
      Text.writeString(out, values[i].getClass().getName());
    }
    // 仅写入存在元素的序列化内容
    for (int i = 0; i < values.length; ++i) {
      if (has(i)) {
        values[i].write(out);
      }
    }
  }

  /**
   * 从输入流反序列化元组
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked") // No static typeinfo on Tuples
  public void readFields(DataInput in) throws IOException {
    // 读取元组容量
    int card = WritableUtils.readVInt(in);
    values = new Writable[card];
    // 读取存在标记位
    readBitSet(in, card, written);
    // 存储每个元素的类对象
    Class<? extends Writable>[] cls = new Class[card];
    try {
      // 读取每个元素的类名并加载类
      for (int i = 0; i < card; ++i) {
        cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
      }
      // 实例化每个元素
      for (int i = 0; i < card; ++i) {
        // NullWritable使用单例
        if (cls[i].equals(NullWritable.class)) {
          values[i] = NullWritable.get();
        } else {
          // 通过反射创建实例
          values[i] = cls[i].newInstance();
        }
        // 仅对存在元素反序列化
        if (has(i)) {
          values[i].readFields(in);
        }
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed tuple init", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Failed tuple init", e);
    } catch (InstantiationException e) {
      throw new IOException("Failed tuple init", e);
    }
  }

  /**
   * 标记指定位置存在有效值
   * @param i 要标记的位置索引
   */
  void setWritten(int i) {
    written.set(i);
  }

  /**
   * 标记指定位置不存在有效值
   * @param i 要清除标记的位置索引
   */
  void clearWritten(int i) {
    written.clear(i);
  }

  /**
   * 清除所有存在标记，不释放存储空间
   */
  void clearWritten() {
    written.clear();
  }

  /**
   * 将存在标记位写入输出流，兼容旧版本格式：
   * 前64位用VLong存储（兼容旧版本TupleWritable），超过64位的部分每8位用一个字节存储
   * @param stream 输出流
   * @param nbits 总位数
   * @param bitSet 要写入的BitSet
   * @throws IOException 写入失败抛出异常
   */
  private static final void writeBitSet(DataOutput stream, int nbits,
      BitSet bitSet) throws IOException {
    long bits = 0L;
        
    // 处理前64位，存入VLong
    int bitSetIndex = bitSet.nextSetBit(0);
    for (;bitSetIndex >= 0 && bitSetIndex < Long.SIZE;
            bitSetIndex=bitSet.nextSetBit(bitSetIndex+1)) {
      bits |= 1L << bitSetIndex;
    }
    WritableUtils.writeVLong(stream,bits);
    
    // 处理超过64位的部分，每8位一个字节
    if (nbits > Long.SIZE) {
      bits = 0L;
      for (int lastWordWritten = 0; bitSetIndex >= 0 && bitSetIndex < nbits; 
              bitSetIndex = bitSet.nextSetBit(bitSetIndex+1)) {
        int bitsIndex = bitSetIndex % Byte.SIZE;
        int word = (bitSetIndex-Long.SIZE) / Byte.SIZE;
        if (word > lastWordWritten) {
          stream.writeByte((byte)bits);
          bits = 0L;
          for (lastWordWritten++;lastWordWritten<word;lastWordWritten++) {
            stream.writeByte((byte)bits);
          }
        }
        bits |= 1L << bitsIndex;
      }
      stream.writeByte((byte)bits);
    }
  }

  /**
   * 从输入流读取存在标记位，读取与writeBitSet对应的格式
   * @param stream 输入流
   * @param nbits 总位数
   * @param bitSet 存储结果的BitSet
   * @throws IOException 读取失败抛出异常
   */
  private static final void readBitSet(DataInput stream, int nbits, 
      BitSet bitSet) throws IOException {
    bitSet.clear();
    // 读取前64位
    long initialBits = WritableUtils.readVLong(stream);
    long last = 0L;
    // 逐个提取置位位
    while (0L != initialBits) {
      last = Long.lowestOneBit(initialBits);
      initialBits ^= last;
      bitSet.set(Long.numberOfTrailingZeros(last));
    }
    
    // 读取超过64位的部分，每字节8位
    for (int offset=Long.SIZE; offset < nbits; offset+=Byte.SIZE) {
      byte bits = stream.readByte();
      while (0 != bits) {
        last = Long.lowestOneBit(bits);
        bits ^= last;
        bitSet.set(Long.numberOfTrailingZeros(last) + offset);
      }
    }
  }
}