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

import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * 文件说明：MapReduce旧版API中基于ArrayList实现的可重置迭代器
 * 
 * 该类提供了ResetableIterator接口的实现，使用ArrayList存储添加的元素，
 * 可根据请求重新遍历已存储的元素。优先推荐使用StreamBackedIterator。
 * 继承了新版MapReduce API的ArrayListBackedIterator实现，适配旧版接口。
 * 主要用于MapReduce连接操作中，支持对多个数据源的元组进行重复遍历。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayListBackedIterator<X extends Writable> extends 
    org.apache.hadoop.mapreduce.lib.join.ArrayListBackedIterator<X>
    implements ResetableIterator<X> {

  /**
   * 构造空的基于ArrayList的可重置迭代器
   */
  public ArrayListBackedIterator() {
    super();
  }

  /**
   * 使用已有的ArrayList数据构造可重置迭代器
   * @param data 存储待迭代元素的ArrayList
   */
  public ArrayListBackedIterator(ArrayList<X> data) {
    super(data);
  }
}