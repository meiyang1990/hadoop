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

package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 文件名称: BinaryPartitioner.java
 * 所属模块: MapReduce 核心分区模块
 * 核心职责: 对二进制可比类型键，基于键字节数组的指定子区间进行哈希分区，控制Map输出数据分发到哪个Reduce分区
 * <p>Partition {@link BinaryComparable} keys using a configurable part of 
 * the bytes array returned by {@link BinaryComparable#getBytes()}.</p>
 * 
 * <p>The subarray to be used for the partitioning can be defined by means
 * of the following properties:
 * <ul>
 *   <li>
 *     <i>mapreduce.partition.binarypartitioner.left.offset</i>:
 *     left offset in array (0 by default)
 *   </li>
 *   <li>
 *     <i>mapreduce.partition.binarypartitioner.right.offset</i>: 
 *     right offset in array (-1 by default)
 *   </li>
 * </ul>
 * Like in Python, both negative and positive offsets are allowed, but
 * the meaning is slightly different. In case of an array of length 5,
 * for instance, the possible offsets are:
 * <pre><code>
 *  +---+---+---+---+---+
 *  | B | B | B | B | B |
 *  +---+---+---+---+---+
 *    0   1   2   3   4
 *   -5  -4  -3  -2  -1
 * </code></pre>
 * The first row of numbers gives the position of the offsets 0...5 in 
 * the array; the second row gives the corresponding negative offsets. 
 * Contrary to Python, the specified subarray has byte <code>i</code> 
 * and <code>j</code> as first and last element, repectively, when 
 * <code>i</code> and <code>j</code> are the left and right offset.
 * 
 * <p>For Hadoop programs written in Java, it is advisable to use one of 
 * the following static convenience methods for setting the offsets:
 * <ul>
 *   <li>{@link #setOffsets}</li>
 *   <li>{@link #setLeftOffset}</li>
 *   <li>{@link #setRightOffset}</li>
 * </ul>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BinaryPartitioner<V> extends Partitioner<BinaryComparable, V> 
  implements Configurable {

  // 左偏移量配置项名称
  public static final String LEFT_OFFSET_PROPERTY_NAME = 
    "mapreduce.partition.binarypartitioner.left.offset";
  // 右偏移量配置项名称
  public static final String RIGHT_OFFSET_PROPERTY_NAME = 
    "mapreduce.partition.binarypartitioner.right.offset";
  
  /**
   * 设置分区所用子字节数组的左右偏移量，Python风格切片语法bytes[left:(right+1)]
   * @param conf 配置对象
   * @param left 左偏移量（支持正负）
   * @param right 右偏移量（支持正负）
   */
  public static void setOffsets(Configuration conf, int left, int right) {
    conf.setInt(LEFT_OFFSET_PROPERTY_NAME, left);
    conf.setInt(RIGHT_OFFSET_PROPERTY_NAME, right);
  }
  
  /**
   * 设置分区所用子字节数组的左偏移量，Python风格切片语法bytes[offset:]
   * @param conf 配置对象
   * @param offset 左偏移量（支持正负）
   */
  public static void setLeftOffset(Configuration conf, int offset) {
    conf.setInt(LEFT_OFFSET_PROPERTY_NAME, offset);
  }
  
  /**
   * 设置分区所用子字节数组的右偏移量，Python风格切片语法bytes[:(offset+1)]
   * @param conf 配置对象
   * @param offset 右偏移量（支持正负）
   */
  public static void setRightOffset(Configuration conf, int offset) {
    conf.setInt(RIGHT_OFFSET_PROPERTY_NAME, offset);
  }
  
  
  private Configuration conf;
  private int leftOffset, rightOffset;
  
  /**
   * 注入配置并读取分区偏移量配置
   * @param conf Hadoop配置对象
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    // 读取左偏移量，默认值0
    leftOffset = conf.getInt(LEFT_OFFSET_PROPERTY_NAME, 0);
    // 读取右偏移量，默认值-1
    rightOffset = conf.getInt(RIGHT_OFFSET_PROPERTY_NAME, -1);
  }
  
  /**
   * 获取当前配置对象
   * @return 当前Hadoop配置对象
   */
  public Configuration getConf() {
    return conf;
  }
  
  /** 
   * 根据键的指定字节子区间计算哈希，得到目标分区编号
   * @param key 二进制键对象
   * @param value 键对应的值
   * @param numPartitions 总分区数
   * @return 目标分区编号
   */
  @Override
  public int getPartition(BinaryComparable key, V value, int numPartitions) {
    // 获取键总字节长度
    int length = key.getLength();
    // 计算实际左索引，支持正负偏移转合法数组下标
    int leftIndex = (leftOffset + length) % length;
    // 计算实际右索引，支持正负偏移转合法数组下标
    int rightIndex = (rightOffset + length) % length;
    // 对指定子区间计算哈希值
    int hash = WritableComparator.hashBytes(key.getBytes(), 
      leftIndex, rightIndex - leftIndex + 1);
    // 对哈希值取模得到分区编号，保证结果非负
    return (hash & Integer.MAX_VALUE) % numPartitions;
  }
  
}