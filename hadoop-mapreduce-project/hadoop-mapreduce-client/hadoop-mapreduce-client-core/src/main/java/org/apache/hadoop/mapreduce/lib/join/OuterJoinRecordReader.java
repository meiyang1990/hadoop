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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 全外连接记录读取器，实现MapReduce端的全外连接逻辑
 * 全外连接会保留所有输入中匹配和不匹配的记录，对于不匹配的键对应位置填充空值
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OuterJoinRecordReader<K extends WritableComparable<?>>
    extends JoinRecordReader<K> {

  /**
   * 构造全外连接记录读取器
   * @param id 读取器标识ID
   * @param conf 作业配置对象
   * @param capacity 元组容纳的最大元素数量
   * @param cmpcl 键比较器类
   * @throws IOException 初始化过程中发生IO异常
   */
  OuterJoinRecordReader(int id, Configuration conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, conf, capacity, cmpcl);
  }

  /**
   * 组合不同来源的记录，全外连接总是输出所有收集到的元组
   * 无论键是否在所有输入都存在，都输出该元组
   * @param srcs 不同输入来源的记录值数组
   * @param dst 输出结果元组
   * @return 总是返回true，表示所有组合都需要输出
   */
  protected boolean combine(Object[] srcs, TupleWritable dst) {
    assert srcs.length == dst.size();
    return true;
  }
}