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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.iterators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.yarn.api.records.SchedulingRequest;

/**
 * 按照调度请求到达的原始顺序进行顺序遍历的迭代器
 * 用于YARN调度约束算法中，按输入顺序逐个处理调度请求
 */
public class SerialIterator implements Iterator<SchedulingRequest> {

  private final List<SchedulingRequest> schedulingRequestList;
  private int cursor;

  /**
   * 构造顺序遍历迭代器，基于输入的调度请求集合
   * @param schedulingRequests 待遍历的调度请求集合
   */
  public SerialIterator(Collection<SchedulingRequest> schedulingRequests) {
    this.schedulingRequestList = new ArrayList<>(schedulingRequests);
    this.cursor = 0;
  }

  @Override
  public boolean hasNext() {
    return (cursor < schedulingRequestList.size());
  }

  @Override
  public SchedulingRequest next() {
    if (hasNext()) {
      return schedulingRequestList.get(cursor++);
    }
    throw new NoSuchElementException();
  }
}