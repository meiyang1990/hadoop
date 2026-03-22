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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

/**
 * 内连接记录阅读器，实现MapReduce连接组合中的内连接逻辑
 * 仅保留所有输入数据源中都存在相同键的记录，符合关系型数据库内连接语义
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InnerJoinRecordReader<K extends WritableComparable>
    extends JoinRecordReader<K> {

  /**
   * 构造内连接记录阅读器实例
   * @param id 记录阅读器编号
   * @param conf 作业配置对象
   * @param capacity 输入数据源数量
   * @param cmpcl 键比较器类
   * @throws IOException 构造过程中IO异常
   */
  InnerJoinRecordReader(int id, JobConf conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, conf, capacity, cmpcl);
  }

  /**
   * 检查当前元组是否满足内连接条件，只有所有数据源都包含当前键时才保留
   * @param srcs 各输入数据源对应的记录数组
   * @param dst 输出组合元组
   * @return 所有数据源都包含当前键返回true，否则返回false
   */
  protected boolean combine(Object[] srcs, TupleWritable dst) {
    assert srcs.length == dst.size();
    // 遍历检查所有数据源位置是否都有值
    for (int i = 0; i < srcs.length; ++i) {
      if (!dst.has(i)) {
        return false;
      }
    }
    return true;
  }
}