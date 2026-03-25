// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import java.util.Iterator;

/**
 * 环形迭代器，基于现有迭代器的当前位置开始循环遍历整个可迭代集合
 * @param <T> 迭代元素类型
 */
class CircularIterator<T> {
  private Iterator<T> iterator = null;
  private final Iterable<T> iterable;

  private T startElem = null;
  private T nextElem = null;

  // 如果非空，则覆盖默认起始元素
  private T firstElem = null;

  // 不支持空集合或null集合
  /**
   * 构造环形迭代器，从指定位置开始循环遍历
   * @param first 优先返回的首个元素，若为null则按原始位置遍历
   * @param iter 原始迭代器，指定起始遍历位置
   * @param iterable 完整可迭代集合，用于遍历完后从头开始循环
   */
  CircularIterator(T first, Iterator<T> iter,
      Iterable<T> iterable) {
    this.firstElem = first;
    this.iterable = iterable;
    if (!iter.hasNext()) {
      this.iterator = this.iterable.iterator();
    } else {
      this.iterator = iter;
    }
    this.startElem = this.iterator.next();
    this.nextElem = this.startElem;
  }

  /**
   * 检查是否还有未遍历的元素（未回到起始点则存在）
   * @return true 还有元素可遍历；false 已遍历完一圈回到起点
   */
  boolean hasNext() {
    // 已有预取元素或待优先返回的首个元素，直接返回true
    if (this.nextElem != null || this.firstElem != null) {
      return true;
    } else {
      // 当前迭代器还有元素，尝试取下一个
      if (this.iterator.hasNext()) {
        T next = this.iterator.next();
        // 回到起始点，遍历完一圈，结束
        if (this.startElem.equals(next)) {
          return false;
        } else {
          // 预取该元素，标记为可返回
          this.nextElem = next;
          return true;
        }
      } else {
        // 当前迭代器已遍历完，重置到集合开头继续遍历
        this.iterator = this.iterable.iterator();
        this.nextElem = this.iterator.next();
        // 回到起始点，遍历完一圈，结束
        if (this.startElem.equals(this.nextElem)) {
          return false;
        }
        return true;
      }
    }
  }

  /**
   * 获取下一个遍历元素
   * @return 下一个元素
   */
  T next() {
    T retVal;
    // 优先返回指定的首个元素
    if (this.firstElem != null) {
      retVal = this.firstElem;
      this.firstElem = null;
    } else if (this.nextElem != null) {
      // 返回预取好的下一个元素
      retVal = this.nextElem;
      this.nextElem = null;
    } else {
      // 直接从当前迭代器获取下一个元素
      retVal = this.iterator.next();
    }
    return retVal;
  }
}