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
 * 内连接记录读取器，实现MapReduce端的内连接逻辑
 * 仅保留所有输入分片中都存在相同键的记录
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InnerJoinRecordReader<K extends WritableComparable<?>>
    extends JoinRecordReader<K> {

  /**
   * 构造内连接记录读取器实例
   * @param id 读取器标识ID
   * @param conf Hadoop配置对象
   * @param capacity 输入分片数量
   * @param cmpcl 键比较器类
   * @throws IOException 初始化异常
   */
  InnerJoinRecordReader(int id, Configuration conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, conf, capacity, cmpcl);
  }

  /**
   * 检查当前键是否满足内连接条件，所有数据源都包含该键才输出
   * @param srcs 各数据源的当前值数组
   * @param dst 输出结果元组
   * @return 所有数据源都包含当前键返回true，否则返回false
   */
  protected boolean combine(Object[] srcs, TupleWritable dst) {
    assert srcs.length == dst.size();
    for (int i = 0; i < srcs.length; ++i) {
      // 检查当前数据源是否包含该键
      if (!dst.has(i)) {
        return false;
      }
    }
    // 所有数据源都包含该键，满足内连接条件
    return true;
  }
}