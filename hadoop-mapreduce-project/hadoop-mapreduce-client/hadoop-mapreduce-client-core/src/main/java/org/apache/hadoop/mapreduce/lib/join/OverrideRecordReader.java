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

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件：OverrideRecordReader.java
 * 所属模块：MapReduce核心客户端，MapReduce连接操作模块
 * 类功能说明：实现覆盖式连接的RecordReader，对于相同键优先采用最右侧数据源的值覆盖左侧数据源，用于数据补全/更新场景。
 * 例如 override(S1,S2,S3) 对所有相同键，优先使用S3的值，其次S2，最后S1。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OverrideRecordReader<K extends WritableComparable<?>,
                                  V extends Writable>
    extends MultiFilterRecordReader<K,V> {

  /**
   * 构造覆盖式连接RecordReader
   * @param id 当前Reader标识ID
   * @param conf Hadoop配置对象
   * @param capacity 初始容量
   * @param cmpcl 键比较器类
   * @throws IOException IO异常
   */
  OverrideRecordReader(int id, Configuration conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, conf, capacity, cmpcl);
  }
  private Class<? extends Writable> valueclass = null;

  /**
   * 获取要输出的值，提取元组中优先级最高（最右侧数据源）的值
   * @param dst 存储多数据源值的元组
   * @return 优先级最高的值
   */
  @SuppressWarnings("unchecked") // No static typeinfo on Tuples
  protected V emit(TupleWritable dst) {
    return (V) dst.iterator().next();
  }

  /**
   * 创建值对象实例，从最右侧非空数据源获取值类型
   * @return 新建的值对象实例
   */
  @SuppressWarnings("unchecked") // Explicit check for value class agreement
  public V createValue() {
    if (null == valueclass) {
      // 从最右侧数据源开始查找第一个非NullWritable的值类型
      Class<?> cls = kids[kids.length -1].createValue().getClass();
      for (int i = kids.length -1; cls.equals(NullWritable.class); i--) {
        cls = kids[i].createValue().getClass();
      }
      valueclass = cls.asSubclass(Writable.class);
    }
    if (valueclass.equals(NullWritable.class)) {
      return (V) NullWritable.get();
    }
    // 反射创建值实例
    return (V) ReflectionUtils.newInstance(valueclass, null);
  }

  /**
   * 填充连接收集器，仅保留当前键最右侧数据源的值，跳过其他数据源
   * 该设计节省空间，同时避免产生笛卡尔积，输出数量等于最高优先级数据源的键值对数量
   * @param iterkey 当前处理的键
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  protected void fillJoinCollector(K iterkey) 
      throws IOException, InterruptedException {
    final PriorityQueue<ComposableRecordReader<K,?>> q = 
      getRecordReaderQueue();
    if (q != null && !q.isEmpty()) {
      // 记录最高优先级Reader在列表中的位置
      int highpos = -1;
      ArrayList<ComposableRecordReader<K,?>> list =
        new ArrayList<ComposableRecordReader<K,?>>(kids.length);
      // 设置当前迭代键
      q.peek().key(iterkey);
      final WritableComparator cmp = getComparator();
      // 收集所有当前键相等的Reader
      while (0 == cmp.compare(q.peek().key(), iterkey)) {
        ComposableRecordReader<K,?> t = q.poll();
        // 找到ID更大（更靠右，优先级更高）的Reader
        if (-1 == highpos || list.get(highpos).id() < t.id()) {
          highpos = list.size();
        }
        list.add(t);
        if (q.isEmpty())
          break;
      }
      // 只将最高优先级Reader加入连接收集器
      ComposableRecordReader<K,?> t = list.remove(highpos);
      t.accept(jc, iterkey);
      // 其他同键Reader跳过当前键，不加入连接结果
      for (ComposableRecordReader<K,?> rr : list) {
        rr.skip(iterkey);
      }
      list.add(t);
      // 将还有后续数据的Reader重新放回优先级队列
      for (ComposableRecordReader<K,?> rr : list) {
        if (rr.hasNext()) {
          q.add(rr);
        }
      }
    }
  }

}