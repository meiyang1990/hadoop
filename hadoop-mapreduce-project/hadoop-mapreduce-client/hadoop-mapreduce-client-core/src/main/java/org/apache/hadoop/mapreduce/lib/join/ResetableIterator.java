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
import org.apache.hadoop.io.Writable;

/**
 * 文件说明：MapReduce Join操作框架的可重置迭代器接口，定义了支持重放元素的有状态迭代器规范
 * 
 * 该接口定义了一种支持从开头重新遍历的有状态迭代器，用于MapReduce的多输入数据连接操作中，
 * 允许对已添加的元素进行重复遍历，满足不同Join算法的重访问需求。
 * 注意：该接口不继承自标准的{@link java.util.Iterator}。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ResetableIterator<T extends Writable> {

  /**
   * 空迭代器实现，作为默认空实例，用于表示无数据的迭代场景
   * @param <U> 迭代元素类型，必须继承Writable
   */
  public static class EMPTY<U extends Writable>
    implements ResetableIterator<U> {
    public boolean hasNext() { return false; }
    public void reset() { }
    public void close() throws IOException { }
    public void clear() { }
    public boolean next(U val) throws IOException {
      return false;
    }
    public boolean replay(U val) throws IOException {
      return false;
    }
    public void add(U item) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * 检查是否还有下一个元素可供遍历
   * 允许存在误判为true（假阳性），但不允许存在误判为false（假阴性）
   * @return 如果有下一个元素返回true，否则返回false
   */
  public boolean hasNext();

  /**
   * 获取下一个元素，将值写入传入的对象中
   * 调用{@link #reset()}后，必须按照添加元素的顺序（FIFO）重新返回元素，
   * 嵌套Join场景下可能返回false，表示没有满足Join约束的元素
   * @param val 用于存储下一个元素值的对象
   * @return 成功获取下一个元素返回true，无元素返回false
   * @throws IOException IO异常
   */
  public boolean next(T val) throws IOException;

  /**
   * 将上一次返回的元素值重新写入传入对象，实现元素重放
   * @param val 用于存储重放元素值的对象
   * @return 成功重放元素返回true，否则返回false
   * @throws IOException IO异常
   */
  public boolean replay(T val) throws IOException;

  /**
   * 将迭代器重置回起始位置，从第一个元素开始重新遍历
   * 必须在调用{@link #add}之后调用本方法，避免触发并发修改异常
   */
  public void reset();

  /**
   * 向迭代器集合中添加一个待遍历的元素
   * @param item 待添加的元素
   * @throws IOException IO异常
   */
  public void add(T item) throws IOException;

  /**
   * 关闭数据源并释放迭代器占用的所有资源，关闭后调用迭代器方法行为未定义
   * @throws IOException IO异常
   */
  // XXX is this necessary?
  public void close() throws IOException;

  /**
   * 清空迭代器数据，但不释放内部资源，清空后迭代器可复用连接新数据源
   */
  public void clear();

}