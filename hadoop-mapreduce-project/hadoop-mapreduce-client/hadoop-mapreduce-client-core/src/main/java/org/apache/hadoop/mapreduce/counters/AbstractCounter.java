// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.counters;

import org.apache.hadoop.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.Counter;

/**
 * MapReduce计数器抽象基类，为mapred和mapreduce两个包提供Counter接口的公共基础实现
 * 定义了计数器通用的相等判断、哈希计算等公共逻辑，具体计数器实现可继承此类复用代码
 */
@InterfaceAudience.Private
public abstract class AbstractCounter implements Counter {

  @Override @Deprecated
  public void setDisplayName(String name) {}

  /**
   * 比较两个计数器是否相等，基于名称、显示名和计数器值三个属性判断
   * @param genericRight 待比较的另一个对象
   * @return 相等返回true，否则返回false
   */
  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof Counter) {
      synchronized (genericRight) {
        Counter right = (Counter) genericRight;
        return getName().equals(right.getName()) &&
               getDisplayName().equals(right.getDisplayName()) &&
               getValue() == right.getValue();
      }
    }
    return false;
  }

  /**
   * 计算计数器的哈希值，基于名称、显示名和计数器值生成
   * @return 计数器的哈希值
   */
  @Override
  public synchronized int hashCode() {
    return Objects.hashCode(getName(), getDisplayName(), getValue());
  }
}