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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * YARN时间线服务HBase存储层的范围封装类，封装带起始和结束索引的区间。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Range {
  private final int startIdx;
  private final int endIdx;

  /**
   * 构造一个左闭右开区间 [start, end)。
   *
   * @param start 起始索引（包含）
   * @param end 结束索引（不包含）
   */
  public Range(int start, int end) {
    // 参数合法性校验，要求0 <= 起始 <= 结束
    if (start < 0 || end < start) {
      throw new IllegalArgumentException(
          "Invalid range, required that: 0 <= start <= end; start=" + start
              + ", end=" + end);
    }

    this.startIdx = start;
    this.endIdx = end;
  }

  /**
   * 获取区间起始索引。
   * @return 起始索引值
   */
  public int start() {
    return startIdx;
  }

  /**
   * 获取区间结束索引。
   * @return 结束索引值
   */
  public int end() {
    return endIdx;
  }

  /**
   * 获取区间长度。
   * @return 区间包含的元素个数
   */
  public int length() {
    return endIdx - startIdx;
  }
}