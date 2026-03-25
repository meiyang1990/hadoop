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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.yarn.api.records.SchedulingRequest;

/**
 * YARN调度请求迭代器，按标签流行度降序遍历调度请求。
 * 优先处理携带更流行（出现次数更多）标签的调度请求，当前统计当前批次内的流行度，可扩展为全局统计。
 */
public class PopularTagsIterator implements Iterator<SchedulingRequest> {

  private final List<SchedulingRequest> schedulingRequestList;
  private int cursor;

  /**
   * 构造按标签流行度排序的迭代器，对传入调度请求按标签流行度降序排序。
   * @param schedulingRequests 待遍历的调度请求集合
   */
  public PopularTagsIterator(Collection<SchedulingRequest> schedulingRequests) {
    this.schedulingRequestList = new ArrayList<>(schedulingRequests);
    // 按标签流行度降序排序，越流行越靠前
    Collections.sort(schedulingRequestList,
        (o1, o2) -> (int) getTagPopularity(o2) - (int) getTagPopularity(o1));

    this.cursor = 0;
  }

  /**
   * 计算单个调度请求的标签流行度，取该请求所有标签中出现次数最高的次数作为流行度。
   * @param request 待计算的调度请求
   * @return 该请求的最高标签流行度
   */
  private long getTagPopularity(SchedulingRequest request) {
    long maxCount = 0;
    // 遍历该请求的所有标签，统计每个标签在当前批次中的出现次数
    for (String tag : request.getAllocationTags()) {
      long count = schedulingRequestList.stream()
          .filter(req -> req.getAllocationTags().contains(tag)).count();
      // 更新最大流行度
      if (count > maxCount) {
        maxCount = count;
      }
    }
    return maxCount;
  }

  @Override
  public boolean hasNext() {
    // 检查是否还有未遍历的调度请求
    return (cursor < schedulingRequestList.size());
  }

  @Override
  public SchedulingRequest next() {
    if (hasNext()) {
      // 返回当前位置请求，游标后移
      return schedulingRequestList.get(cursor++);
    }
    // 无剩余元素抛出异常
    throw new NoSuchElementException();
  }
}