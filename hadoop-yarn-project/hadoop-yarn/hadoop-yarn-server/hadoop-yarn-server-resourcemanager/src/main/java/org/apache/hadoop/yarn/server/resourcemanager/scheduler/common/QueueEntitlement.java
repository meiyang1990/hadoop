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
 
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

/**
 * 队列资源配额定义，存储队列的最小和最大资源容量占比配置
 */
public class QueueEntitlement {

  private float capacity;
  private float maxCapacity;

  /**
   * 构造队列资源配额实例
   * @param capacity 最小资源容量占比（保证分配）
   * @param maxCapacity 最大资源容量占比（上限限制）
   */
  public QueueEntitlement(float capacity, float maxCapacity){
    this.setCapacity(capacity);
    this.maxCapacity = maxCapacity;
   }

  /**
   * 获取队列最大资源容量占比
   * @return 最大资源容量占比
   */
  public float getMaxCapacity() {
    return maxCapacity;
  }

  public void setMaxCapacity(float maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  /**
   * 获取队列最小资源容量占比
   * @return 最小资源容量占比
   */
  public float getCapacity() {
    return capacity;
  }

  public void setCapacity(float capacity) {
    this.capacity = capacity;
  }

  @Override
  public boolean equals(Object o) {
    // 同一对象直接返回相等
    if (this == o)
      return true;
    // 类型不同直接返回不相等
    if (!(o instanceof QueueEntitlement))
      return false;

    QueueEntitlement that = (QueueEntitlement) o;

    // 比较容量占比是否相等
    if (Float.compare(that.capacity, capacity) != 0)
      return false;
    // 比较最大容量占比是否相等
    return Float.compare(that.maxCapacity, maxCapacity) == 0;
  }

  @Override
  public int hashCode() {
    // 计算容量的哈希值
    int result = (capacity != +0.0f ? Float.floatToIntBits(capacity) : 0);
    // 组合最大容量计算最终哈希值
    result = 31 * result + (maxCapacity != +0.0f ? Float.floatToIntBits(
        maxCapacity) : 0);
    return result;
  }
}