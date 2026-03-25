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

package org.apache.hadoop.mapreduce.lib.aggregate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件：UniqValueCount.java
 * 所属模块：MapReduce 聚合工具库
 * 核心功能：实现去重计数聚合器，统计输入数据中唯一值的数量，支持在MapReduce聚合流程中使用
 * 用于MapReduce的聚合计算场景，对输入对象进行去重并统计唯一值总数
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UniqValueCount implements ValueAggregator<Object> {
  /** 最大唯一值数量配置项名称 */
  public static final String MAX_NUM_UNIQUE_VALUES = 
    "mapreduce.aggregate.max.num.unique.values";

  private TreeMap<Object, Object> uniqItems = null;

  private long numItems = 0;
  
  private long maxNumItems = Long.MAX_VALUE;

  /**
   * 默认构造函数，不限制最大唯一值数量
   */
  public UniqValueCount() {
    this(Long.MAX_VALUE);
  }
  
  /**
   * 带最大唯一值限制的构造函数
   * @param maxNum 需要保留的最大唯一值数量限制
   */
  public UniqValueCount(long maxNum) {
    uniqItems = new TreeMap<Object, Object>();
    this.numItems = 0;
    maxNumItems = Long.MAX_VALUE;
    if (maxNum > 0 ) {
      this.maxNumItems = maxNum;
    }
  }

  /**
   * 设置唯一值数量的上限
   * @param n 期望的最大唯一值数量限制
   * @return 设置后的实际最大限制
   */
  public long setMaxItems(long n) {
    if (n >= numItems) {
      this.maxNumItems = n;
    } else if (this.maxNumItems >= this.numItems) {
      this.maxNumItems = this.numItems;
    }
    return this.maxNumItems;
  }
  
  /**
   * 添加一个新值到聚合器，自动去重
   * @param val 需要添加的对象值
   */
  public void addNextValue(Object val) {
    if (this.numItems <= this.maxNumItems) {
      uniqItems.put(val.toString(), "1");
      this.numItems = this.uniqItems.size();
    }
  }

  /**
   * 获取聚合结果报告，返回当前唯一值的总数量
   * @return 唯一值总数的字符串表示
   */
  public String getReport() {
    return "" + uniqItems.size();
  }

  /**
   * 获取所有去重后的唯一值集合
   * @return 去重后的唯一值集合
   */
  public Set<Object> getUniqueItems() {
    return uniqItems.keySet();
  }

  /**
   * 重置聚合器，清空所有已存储的唯一值，准备下一轮聚合
   */
  public void reset() {
    uniqItems = new TreeMap<Object, Object>();
  }

  /**
   * 生成Combiner阶段的输出，将所有唯一值整理为列表返回
   * @return 包含所有唯一值的ArrayList，供Combiner合并使用
   */
  public ArrayList<Object> getCombinerOutput() {
    Object key = null;
    Iterator<Object> iter = uniqItems.keySet().iterator();
    ArrayList<Object> retv = new ArrayList<Object>();

    while (iter.hasNext()) {
      key = iter.next();
      retv.add(key);
    }
    return retv;
  }
}