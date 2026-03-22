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

package org.apache.hadoop.mapred.lib.aggregate;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 用于去重统计唯一值数量的值聚合器实现，为旧版MapReduce API提供适配
 * 该聚合器对输入的对象序列去重，最终统计得到不重复值的总数
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UniqValueCount 
    extends org.apache.hadoop.mapreduce.lib.aggregate.UniqValueCount 
    implements ValueAggregator<Object> {
  /**
   * 默认构造方法
   */
  public UniqValueCount() {
    super();
  }
  
  /**
   * 带最大唯一值数量限制的构造方法
   * @param maxNum 需要保留的唯一值数量上限
   */
  public UniqValueCount(long maxNum) {
    super(maxNum);
  }
}