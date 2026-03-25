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

package org.apache.hadoop.mapred.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 多输入分片组合容器，用于将多个子InputSplit聚合为一个分片，供MapReduce连接操作使用。
 * 被加入容器的所有子分片都必须拥有公开默认构造函数，支持序列化反序列化。
 * 该类是MapReduce端连接操作的核心数据结构，用于将多个数据源同位置的分片组合，实现数据本地化连接。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CompositeInputSplit implements InputSplit {

  private int fill = 0;
  private long totsize = 0L;
  private InputSplit[] splits;

  /**
   * 默认空构造函数，满足Writable序列化要求
   */
  public CompositeInputSplit() { }

  /**
   * 构造指定容量的组合分片，预分配子分片数组空间
   * @param capacity 可容纳的最大子分片数量
   */
  public CompositeInputSplit(int capacity) {
    splits = new InputSplit[capacity];
  }

  /**
   * 向组合分片容器中添加一个子InputSplit
   * @param s 待添加的子InputSplit
   * @throws IOException 如果容器未初始化或已达到容量上限抛出异常
   */
  public void add(InputSplit s) throws IOException {
    if (null == splits) {
      throw new IOException("Uninitialized InputSplit");
    }
    if (fill == splits.length) {
      throw new IOException("Too many splits");
    }
    splits[fill++] = s;
    totsize += s.getLength();
  }

  /**
   * 获取指定索引位置的子InputSplit
   * @param i 子分片索引
   * @return 对应索引的子InputSplit
   */
  public InputSplit get(int i) {
    return splits[i];
  }

  /**
   * 获取所有已添加子分片的总长度之和
   * @return 组合分片总字节长度
   */
  public long getLength() throws IOException {
    return totsize;
  }

  /**
   * 获取指定索引位置子分片的长度
   * @param i 子分片索引
   * @return 对应子分片的字节长度
   */
  public long getLength(int i) throws IOException {
    return splits[i].getLength();
  }

  /**
   * 收集所有子分片的位置信息，合并去重后返回，用于调度时数据本地化
   * @return 所有子分片所在DataNode节点主机名数组，无重复
   */
  public String[] getLocations() throws IOException {
    HashSet<String> hosts = new HashSet<String>();
    for (InputSplit s : splits) {
      String[] hints = s.getLocations();
      if (hints != null && hints.length > 0) {
        for (String host : hints) {
          hosts.add(host);
        }
      }
    }
    return hosts.toArray(new String[hosts.size()]);
  }

  /**
   * 获取指定索引位置子分片的位置信息
   * @param i 子分片索引
   * @return 对应子分片的主机位置数组
   */
  public String[] getLocation(int i) throws IOException {
    return splits[i].getLocations();
  }

  /**
   * 将组合分片序列化输出到DataOutput，格式为：分片数量 -> 各分片类名 -> 各分片序列化数据
   * @param out 输出流
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, splits.length);
    // 先写入所有子分片的类名，用于反序列化时实例化
    for (InputSplit s : splits) {
      Text.writeString(out, s.getClass().getName());
    }
    // 再写入每个子分片自身的序列化数据
    for (InputSplit s : splits) {
      s.write(out);
    }
  }

  /**
   * 从DataInput反序列化读取组合分片数据，先读取类信息实例化对象，再反序列化每个子分片
   * @param in 输入流
   * @throws IOException 如果子分片实例化读取失败抛出异常
   */
  @SuppressWarnings("unchecked")  // Generic array assignment
  public void readFields(DataInput in) throws IOException {
    // 读取子分片总数
    int card = WritableUtils.readVInt(in);
    // 如果数组不存在或大小不匹配则重新分配
    if (splits == null || splits.length != card) {
      splits = new InputSplit[card];
    }
    Class<? extends InputSplit>[] cls = new Class[card];
    try {
      // 先读取所有子分片的类信息
      for (int i = 0; i < card; ++i) {
        cls[i] =
          Class.forName(Text.readString(in)).asSubclass(InputSplit.class);
      }
      // 逐个实例化子分片并反序列化数据
      for (int i = 0; i < card; ++i) {
        splits[i] = ReflectionUtils.newInstance(cls[i], null);
        splits[i].readFields(in);
      }
    } catch (ClassNotFoundException e) {
      throw (IOException)new IOException("Failed split init").initCause(e);
    }
  }

}