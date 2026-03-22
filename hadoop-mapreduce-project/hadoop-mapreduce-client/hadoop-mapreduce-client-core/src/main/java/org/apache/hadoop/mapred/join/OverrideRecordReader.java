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

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

/**
 * 文件说明：MapReduce连接操作中的覆盖式记录读取器，实现优先级覆盖的多数据源连接逻辑
 * 
 * 优先选择最靠右侧数据源的值进行输出。例如，<code>override(S1,S2,S3)</code>会对所有键，
 * 优先选择S3的值，其次S2，最后才选择S1的值输出。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OverrideRecordReader<K extends WritableComparable,
                                  V extends Writable>
    extends MultiFilterRecordReader<K,V> {

  /**
   * 构造覆盖式记录读取器
   * @param id 读取器ID
   * @param conf 作业配置对象
   * @param capacity 连接收集器容量
   * @param cmpcl 键比较器类
   * @throws IOException 如果初始化失败抛出IO异常
   */
  OverrideRecordReader(int id, JobConf conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, conf, capacity, cmpcl);
  }

  /**
   * 获取需要输出的值，选择元组中位置最高（即优先级最高）的数据源值
   * @param dst 包含多个数据源值的元组
   * @return 返回优先级最高的值
   */
  @SuppressWarnings("unchecked") // No static typeinfo on Tuples
  protected V emit(TupleWritable dst) {
    return (V) dst.iterator().next();
  }

  /**
   * 填充连接收集器，仅保留当前键优先级最高（最右侧）数据源的记录，跳过其余数据源的同键记录
   * 这样既节省空间，又避免产生笛卡尔积，输出数量和最高优先级数据源的记录数保持一致
   * @param iterkey 当前处理的键
   * @throws IOException 如果读取记录失败抛出IO异常
   */
  protected void fillJoinCollector(K iterkey) throws IOException {
    // 获取存储所有记录读取器的优先级队列
    final PriorityQueue<ComposableRecordReader<K,?>> q = getRecordReaderQueue();
    if (!q.isEmpty()) {
      // 记录最高优先级数据源在列表中的位置
      int highpos = -1;
      // 存储当前键匹配的所有数据源读取器
      ArrayList<ComposableRecordReader<K,?>> list =
        new ArrayList<ComposableRecordReader<K,?>>(kids.length);
      // 获取队首读取器的键
      q.peek().key(iterkey);
      // 获取键比较器
      final WritableComparator cmp = getComparator();
      // 遍历所有当前键匹配的数据源读取器
      while (0 == cmp.compare(q.peek().key(), iterkey)) {
        ComposableRecordReader<K,?> t = q.poll();
        // 更新最高优先级读取器位置，ID越大优先级越高
        if (-1 == highpos || list.get(highpos).id() < t.id()) {
          highpos = list.size();
        }
        list.add(t);
        if (q.isEmpty())
          break;
      }
      // 取出最高优先级读取器，将当前键加入连接收集器
      ComposableRecordReader<K,?> t = list.remove(highpos);
      t.accept(jc, iterkey);
      // 其余数据源直接跳过当前键，不加入连接收集器
      for (ComposableRecordReader<K,?> rr : list) {
        rr.skip(iterkey);
      }
      // 把最高优先级读取器加回列表
      list.add(t);
      // 将还有剩余记录的读取器重新加回优先级队列
      for (ComposableRecordReader<K,?> rr : list) {
        if (rr.hasNext()) {
          q.add(rr);
        }
      }
    }
  }

}