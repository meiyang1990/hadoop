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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件路径: hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/join/CompositeInputSplit.java
 * 
 * 复合输入分片，用于MapReduce多数据源连接操作，将多个子分片组合为一个分片，保证同一个Map任务处理多个数据源对应分片
 * 所有添加到该集合的分片必须提供公共无参构造函数，支持序列化反序列化
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CompositeInputSplit extends InputSplit implements Writable {

  private int fill = 0;
  private long totsize = 0L;
  private InputSplit[] splits;
  private Configuration conf = new Configuration();

  /**
   * 空构造函数，用于反序列化
   */
  public CompositeInputSplit() { }

  /**
   * 构造指定容量的复合分片
   * @param capacity 可容纳的子分片最大数量
   */
  public CompositeInputSplit(int capacity) {
    splits = new InputSplit[capacity];
  }

  /**
   * 向复合分片中添加一个子分片
   * @throws IOException 如果分片未初始化或已达到容量上限时抛出
   */
  public void add(InputSplit s) throws IOException, InterruptedException {
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
   * 获取指定索引位置的子分片
   * @param i 子分片索引
   * @return 对应子分片
   */
  public InputSplit get(int i) {
    return splits[i];
  }

  /**
   * 获取所有已添加子分片的总大小
   * @return 分片总字节数
   */
  public long getLength() throws IOException {
    return totsize;
  }

  /**
   * 获取指定索引子分片的大小
   * @param i 子分片索引
   * @return 对应子分片字节大小
   */
  public long getLength(int i) throws IOException, InterruptedException {
    return splits[i].getLength();
  }

  /**
   * 收集所有子分片的位置信息（数据所在节点主机），用于任务本地化调度
   * @return 去重后的所有主机位置数组
   */
  public String[] getLocations() throws IOException, InterruptedException {
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
   * 获取指定索引子分片的位置信息
   * @param i 子分片索引
   * @return 对应子分片的主机位置数组
   */
  public String[] getLocation(int i) throws IOException, InterruptedException {
    return splits[i].getLocations();
  }

  /**
   * 将复合分片序列化输出，格式为：<分片数量><分片1类名><分片2类名>...<分片n类名><分片1序列化数据><分片2序列化数据>...<分片n序列化数据>
   */
  @SuppressWarnings("unchecked")
  public void write(DataOutput out) throws IOException {
    // 写入分片总数
    WritableUtils.writeVInt(out, splits.length);
    // 先写入所有分片的类名
    for (InputSplit s : splits) {
      Text.writeString(out, s.getClass().getName());
    }
    // 再序列化每个分片的实际数据
    for (InputSplit s : splits) {
      SerializationFactory factory = new SerializationFactory(conf);
      Serializer serializer = 
        factory.getSerializer(s.getClass());
      serializer.open((DataOutputStream)out);
      serializer.serialize(s);
    }
  }

  /**
   * 从输入流反序列化复合分片
   * @throws IOException 如果读取子分片失败（通常是权限或类找不到问题）抛出
   */
  @SuppressWarnings("unchecked")  // Generic array assignment
  public void readFields(DataInput in) throws IOException {
    // 读取分片总数
    int card = WritableUtils.readVInt(in);
    // 如果分片数组不存在或大小不匹配，重新创建
    if (splits == null || splits.length != card) {
      splits = new InputSplit[card];
    }
    Class<? extends InputSplit>[] cls = new Class[card];
    try {
      // 先读取所有分片的类信息
      for (int i = 0; i < card; ++i) {
        cls[i] =
          Class.forName(Text.readString(in)).asSubclass(InputSplit.class);
      }
      // 反射创建分片实例并反序列化数据
      for (int i = 0; i < card; ++i) {
        splits[i] = ReflectionUtils.newInstance(cls[i], null);
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer deserializer = factory.getDeserializer(cls[i]);
        deserializer.open((DataInputStream)in);
        splits[i] = (InputSplit)deserializer.deserialize(splits[i]);
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed split init", e);
    }
  }
}