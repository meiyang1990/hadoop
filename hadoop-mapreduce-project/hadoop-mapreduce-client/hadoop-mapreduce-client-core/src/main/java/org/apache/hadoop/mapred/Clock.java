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

package org.apache.hadoop.mapred;

/**
 * 时钟工具类，可在单元测试中通过Mock替换实现，便于控制时间逻辑
 * 为MapReduce任务调度等需要获取当前时间的场景提供统一时间入口
 */
class Clock {
  /**
   * 获取当前系统时间的毫秒数
   * @return 当前系统时间，从1970-01-01 UTC开始的毫秒数
   */
  long getTime() {
    return System.currentTimeMillis();
  }
}