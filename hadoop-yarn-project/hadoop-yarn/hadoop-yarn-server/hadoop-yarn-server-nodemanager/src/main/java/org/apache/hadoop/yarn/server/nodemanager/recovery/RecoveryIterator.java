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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * NodeManager恢复数据读取迭代器接口，对原始迭代器进行封装，
 * 将迭代过程中抛出的运行时异常统一转换为IOException，方便错误处理。
 * @param <T> 迭代元素类型
 */
public interface RecoveryIterator<T> extends Closeable {

  /**
   * 检查迭代是否还有更多元素
   * @return 如果还有元素返回true，否则返回false
   * @throws IOException 读取恢复数据时发生I/O错误
   */
  boolean hasNext() throws IOException;

  /**
   * 获取迭代的下一个元素
   * @return 下一个恢复数据元素
   * @throws IOException 读取恢复数据时发生I/O错误
   * @throws NoSuchElementException 迭代已没有更多元素
   */
  T next() throws IOException, NoSuchElementException;

}