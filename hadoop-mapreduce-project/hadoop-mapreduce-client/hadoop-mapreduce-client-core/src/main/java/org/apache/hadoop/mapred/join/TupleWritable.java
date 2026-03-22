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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * MapReduce旧API版元组Writable实现，用于连接框架存储多个Writable对象
 * 
 * 该类并非通用元组类型，仅为MapReduce连接框架设计：
 * 1. 依赖连接框架保障类型安全
 * 2. 假设实例很少需要持久化存储，性能优化方向针对该场景
 * 3. 通用场景下更推荐开发者自定义可序列化类型，可获得更好的验证能力和编码效率
 * 
 * 继承新版TupleWritable实现，兼容旧mapred API
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TupleWritable 
    extends org.apache.hadoop.mapreduce.lib.join.TupleWritable {

  /**
   * 构造空元组，不分配存储空间
   */
  public TupleWritable() {
    super();
  }

  /**
   * 使用给定Writable数组初始化元组，此时不保证各个位置是否包含已写入的值
   * @param vals 存储到元组中的Writable数组
   */
  public TupleWritable(Writable[] vals) {
    super(vals);
  }

  /**
   * 标记指定位置元组包含有效元素
   * @param i 待标记的位置索引
   */
  void setWritten(int i) {
    written.set(i);
  }

  /**
   * 标记指定位置元组不包含有效元素
   * @param i 待清除标记的位置索引
   */
  void clearWritten(int i) {
    written.clear(i);
  }

  /**
   * 清除所有位置的写入标记，不释放原有存储空间
   */
  void clearWritten() {
    written.clear();
  }


}